import Logging
import struct Foundation.Data

extension PostgresConnection: PostgresDatabase {
    public func send(
        _ request: PostgresRequest,
        logger: Logger
    ) -> EventLoopFuture<Void> {
        guard let command = request as? PostgresCommands else {
            preconditionFailure("\(#function) requires an instance of PostgresCommands. This will be a compile-time error in the future.")
        }
        
        let resultFuture: EventLoopFuture<Void>
        
        switch command {
        case .query(let query, let binds, let onMetadata, let onRow):
            resultFuture = self.underlying.query(query, binds, logger: logger).flatMap { rows in
                let fields = rows.rowDescription.map { column in
                    PostgresMessage.RowDescription.Field(
                        name: column.name,
                        tableOID: UInt32(column.tableOID),
                        columnAttributeNumber: column.columnAttributeNumber,
                        dataType: PostgresDataType(UInt32(column.dataType.rawValue)),
                        dataTypeSize: column.dataTypeSize,
                        dataTypeModifier: column.dataTypeModifier,
                        formatCode: .init(psqlFormatCode: column.format)
                    )
                }
                
                let lookupTable = PostgresRow.LookupTable(rowDescription: .init(fields: fields), resultFormat: [.binary])
                return rows.iterateRowsWithoutBackpressureOption(lookupTable: lookupTable, onRow: onRow).map { _ in
                    onMetadata(PostgresQueryMetadata(string: rows.commandTag)!)
                }
            }
        case .prepareQuery(let request):
            resultFuture = self.underlying.prepareStatement(request.query, with: request.name, logger: self.logger).map {
                request.prepared = PreparedQuery(underlying: $0, database: self)
            }
        case .executePreparedStatement(let preparedQuery, let binds, let onRow):
            resultFuture = self.underlying.execute(preparedQuery.underlying, binds, logger: logger).flatMap { rows in
                guard let lookupTable = preparedQuery.lookupTable else {
                    return self.eventLoop.makeSucceededFuture(())
                }
                
                return rows.iterateRowsWithoutBackpressureOption(lookupTable: lookupTable, onRow: onRow)
            }
        }
        
        return resultFuture.flatMapErrorThrowing { error in
            throw error.asAppropriatePostgresError
        }
    }

    public func withConnection<T>(_ closure: (PostgresConnection) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

internal enum PostgresCommands: PostgresRequest {
    case query(query: String,
               binds: [PostgresData],
               onMetadata: (PostgresQueryMetadata) -> () = { _ in },
               onRow: (PostgresRow) throws -> ())
    case prepareQuery(request: PrepareQueryRequest)
    case executePreparedStatement(query: PreparedQuery, binds: [PostgresData], onRow: (PostgresRow) throws -> ())
    
    func respond(to message: PostgresMessage) throws -> [PostgresMessage]? {
        fatalError("This function must not be called")
    }
    
    func start() throws -> [PostgresMessage] {
        fatalError("This function must not be called")
    }
    
    func log(to logger: Logger) {
        fatalError("This function must not be called")
    }
}

extension PSQLRows {
    
    func iterateRowsWithoutBackpressureOption(lookupTable: PostgresRow.LookupTable, onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        self.onRow { psqlRow in
            let columns = psqlRow.data.map { psqlData in
                PostgresMessage.DataRow.Column(value: psqlData.bytes)
            }
            
            let row = PostgresRow(dataRow: .init(columns: columns), lookupTable: lookupTable)
            
            do {
                try onRow(row)
                return self.eventLoop.makeSucceededFuture(Void())
            } catch {
                return self.eventLoop.makeFailedFuture(error)
            }
        }
    }
    
}
