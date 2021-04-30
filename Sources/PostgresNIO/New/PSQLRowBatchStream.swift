import NIO
import Logging

final class PSQLRowBatchStream {
    
    enum RowSource {
        case stream(PSQLRowsDataSource)
        case noRows(Result<String, Error>)
    }
    
    let eventLoop: EventLoop
    let logger: Logger
    
    private enum UpstreamState {
        case streaming(buffer: CircularBuffer<PSQLBackendMessage.DataRow>, dataSource: PSQLRowsDataSource)
        case finished(buffer: CircularBuffer<PSQLBackendMessage.DataRow>, commandTag: String)
        case failure(Error)
        case consumed(Result<String, Error>)
        case modifying
    }
    
    private enum DownstreamState {
        case waitingForNext(EventLoopPromise<PSQLRow?>)
        case waitingForAll(EventLoopPromise<[PSQLRow]>)
        case consuming
    }
    
    internal let rowDescription: [PSQLBackendMessage.RowDescription.Column]
    private let lookupTable: [String: Int]
    private var upstreamState: UpstreamState
    private var downstreamState: DownstreamState
    private let jsonDecoder: PSQLJSONDecoder
    
    init(rowDescription: [PSQLBackendMessage.RowDescription.Column],
         queryContext: ExtendedQueryContext,
         eventLoop: EventLoop,
         rowSource: RowSource)
    {
        let buffer = CircularBuffer<PSQLBackendMessage.DataRow>()
        
        self.downstreamState = .consuming
        switch rowSource {
        case .stream(let dataSource):
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
        case .noRows(.success(let commandTag)):
            self.upstreamState = .finished(buffer: .init(), commandTag: commandTag)
        case .noRows(.failure(let error)):
            self.upstreamState = .failure(error)
        }
        
        self.eventLoop = eventLoop
        self.logger = queryContext.logger
        self.jsonDecoder = queryContext.jsonDecoder
        
        self.rowDescription = rowDescription
        var lookup = [String: Int]()
        lookup.reserveCapacity(rowDescription.count)
        rowDescription.enumerated().forEach { (index, column) in
            lookup[column.name] = index
        }
        self.lookupTable = lookup
    }
    
    func next() -> EventLoopFuture<PSQLRow?> {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.next()
            }
        }
        
        guard case .consuming = self.downstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.upstreamState {
        case .streaming(var buffer, let dataSource):
            self.upstreamState = .modifying
            
            if let data = buffer.popFirst() {
                self.downstreamState = .consuming
                self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
                let row = PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
                return self.eventLoop.makeSucceededFuture(row)
            }
            
            let promise = self.eventLoop.makePromise(of: PSQLRow?.self)
            self.downstreamState = .waitingForNext(promise)
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
            dataSource.request()
            return promise.futureResult
            
        case .finished(var buffer, let commandTag) where buffer.count > 0:
            self.upstreamState = .modifying
            
            let data = buffer.popFirst()!
            self.downstreamState = .consuming
            self.upstreamState = .finished(buffer: buffer, commandTag: commandTag)
            let row = PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
            return self.eventLoop.makeSucceededFuture(row)

        case .finished(let buffer, let commandTag):
            precondition(buffer.isEmpty)
            // no more rows to read... we are finished now
            self.upstreamState = .consumed(.success(commandTag))
            
            return self.eventLoop.makeSucceededFuture(nil)
            
        case .consumed:
            preconditionFailure("We already signaled, that the stream has completed, why are we asked again?")
            
        case .modifying:
            preconditionFailure("Invalid state")
            
        case .failure(let error):
            self.upstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    func all() -> EventLoopFuture<[PSQLRow]> {
        guard self.eventLoop.inEventLoop else {
            return self.eventLoop.flatSubmit {
                self.all()
            }
        }
        
        guard case .consuming = self.downstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.upstreamState {
        case .streaming(_, let dataSource):
            dataSource.request()
            let promise = self.eventLoop.makePromise(of: [PSQLRow].self)
            self.downstreamState = .waitingForAll(promise)
            return promise.futureResult
            
        case .finished(let buffer, let commandTag):
            self.upstreamState = .modifying
            
            let rows = buffer.map {
                PSQLRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
            }
            
            self.downstreamState = .consuming
            self.upstreamState = .consumed(.success(commandTag))
            return self.eventLoop.makeSucceededFuture(rows)
            
        case .consumed:
            preconditionFailure("We already signaled, that the stream has completed, why are we asked again?")
            
        case .modifying:
            preconditionFailure("Invalid state")
            
        case .failure(let error):
            self.upstreamState = .consumed(.failure(error))
            return self.eventLoop.makeFailedFuture(error)
        }
    }
    
    internal func noticeReceived(_ notice: PSQLBackendMessage.NoticeResponse) {
        self.logger.debug("Notice Received", metadata: [
            .notice: "\(notice)"
        ])
    }
    
    internal func receive(_ newRows: CircularBuffer<PSQLBackendMessage.DataRow>) {
        precondition(!newRows.isEmpty, "Expected to get rows!")
        self.eventLoop.preconditionInEventLoop()
        self.logger.trace("Row stream received rows", metadata: [
            "row_count": "\(newRows.count)"
        ])
        
        guard case .streaming(var buffer, let dataSource) = self.upstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.downstreamState {
        case .waitingForNext(let promise):
            precondition(buffer.isEmpty)
            var newRows = newRows
            let data = newRows.popFirst()!
            let row = PSQLRow(data: data, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
            
            self.downstreamState = .consuming
            self.upstreamState = .streaming(buffer: newRows, dataSource: dataSource)
            promise.succeed(row)
        case .waitingForAll:
            self.upstreamState = .modifying
            buffer.append(contentsOf: newRows)
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
            
            // immidiatly request more
            dataSource.request()
        case .consuming:
            // this might happen, if the query has finished while the user is consuming data
            // we don't need to ask for more since the user is consuming anyway
            self.upstreamState = .modifying
            buffer.append(contentsOf: newRows)
            self.upstreamState = .streaming(buffer: buffer, dataSource: dataSource)
        }
    }
    
    internal func receive(completion result: Result<String, Error>) {
        self.eventLoop.preconditionInEventLoop()
        
        guard case .streaming(let oldBuffer, _) = self.upstreamState else {
            preconditionFailure("Invalid state")
        }
        
        switch self.downstreamState {
        case .waitingForNext(let promise):
            precondition(oldBuffer.isEmpty)
            self.downstreamState = .consuming
            self.upstreamState = .consumed(result)
            promise.succeed(nil)
            
        case .consuming:
            switch result {
            case .success(let commandTag):
                self.upstreamState = .finished(buffer: oldBuffer, commandTag: commandTag)
            case .failure(let error):
                self.upstreamState = .failure(error)
            }

        case .waitingForAll(let promise):
            switch result {
            case .failure(let error):
                self.upstreamState = .consumed(.failure(error))
                promise.fail(error)
            case .success(let commandTag):
                let rows = oldBuffer.map {
                    PSQLRow(data: $0, lookupTable: self.lookupTable, columns: self.rowDescription, jsonDecoder: self.jsonDecoder)
                }
                self.upstreamState = .consumed(.success(commandTag))
                promise.succeed(rows)
            }
        }
    }
    
    func cancel() {
        guard case .streaming(_, let dataSource) = self.upstreamState else {
            // We don't need to cancel any upstream resource. All needed data is already
            // included in this 
            return
        }
        
        dataSource.cancel()
    }
    
    var commandTag: String {
        guard case .consumed(.success(let commandTag)) = self.upstreamState else {
            preconditionFailure("commandTag may only be called if all rows have been consumed")
        }
        return commandTag
    }
        
    func onRow(_ onRow: @escaping (PSQLRow) -> EventLoopFuture<Void>) -> EventLoopFuture<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        func consumeNext(promise: EventLoopPromise<Void>) {
            self.next().whenComplete { result in
                switch result {
                case .success(.some(let row)):
                    onRow(row).whenComplete { result in
                        switch result {
                        case .success:
                            consumeNext(promise: promise)
                        case .failure(let error):
                            promise.fail(error)
                        }
                    }
                case .success(.none):
                    promise.succeed(Void())
                case .failure(let error):
                    promise.fail(error)
                }
            }
        }
        
        consumeNext(promise: promise)
        
        return promise.futureResult
    }
}

protocol PSQLRowsDataSource {
    
    func request()
    func cancel()
    
}
