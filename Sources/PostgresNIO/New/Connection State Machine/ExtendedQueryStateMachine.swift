
struct ExtendedQueryStateMachine {
    
    enum State {
        case initialized(ExtendedQueryContext)
        case parseDescribeBindExecuteSyncSent(ExtendedQueryContext)
        
        case parseCompleteReceived(ExtendedQueryContext)
        case parameterDescriptionReceived(ExtendedQueryContext)
        case rowDescriptionReceived(ExtendedQueryContext, [PSQLBackendMessage.RowDescription.Column])
        case noDataMessageReceived(ExtendedQueryContext)
        
        /// A state that is used if a noData message was received before. If a row description was received `bufferingRows` is
        /// used after receiving a `bindComplete` message
        case bindCompleteReceived(ExtendedQueryContext)
        case bufferingRows([PSQLBackendMessage.RowDescription.Column], CircularBuffer<[PSQLData]>, readOnEmpty: Bool)
        case waitingForNextRow([PSQLBackendMessage.RowDescription.Column], CircularBuffer<[PSQLData]>, EventLoopPromise<StateMachineStreamNextResult>)
        
        case commandComplete(commandTag: String)
        case error(PSQLError)
        
        case modifying
    }
    
    enum Action {
        case sendParseDescribeBindExecuteSync(query: String, binds: [PSQLEncodable])
        case sendBindExecuteSync(statementName: String, binds: [PSQLEncodable])
        
        // --- general actions
        case failQuery(ExtendedQueryContext, with: PSQLError)
        case succeedQuery(ExtendedQueryContext, columns: [PSQLBackendMessage.RowDescription.Column])
        case succeedQueryNoRowsComming(ExtendedQueryContext, commandTag: String)
        
        // --- streaming actions
        // actions if query has requested next row but we are waiting for backend
        case forwardRow([PSQLData], to: EventLoopPromise<StateMachineStreamNextResult>)
        case forwardCommandComplete(CircularBuffer<[PSQLData]>, commandTag: String, to: EventLoopPromise<StateMachineStreamNextResult>)
        case forwardStreamError(PSQLError, to: EventLoopPromise<StateMachineStreamNextResult>)
        // actions if query has not asked for next row but are pushing the final bytes to it
        case forwardStreamErrorToCurrentQuery(PSQLError, read: Bool)
        case forwardStreamCompletedToCurrentQuery(CircularBuffer<[PSQLData]>, commandTag: String, read: Bool)

        case read
        case wait
    }
    
    var state: State
    
    init(queryContext: ExtendedQueryContext) {
        self.state = .initialized(queryContext)
    }
    
    mutating func start() -> Action {
        guard case .initialized(let queryContext) = self.state else {
            preconditionFailure("Start should only be called, if the query has been initialized")
        }
        
        switch queryContext.query {
        case .unnamed(let query):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .parseDescribeBindExecuteSyncSent(queryContext)
                return .sendParseDescribeBindExecuteSync(query: query, binds: queryContext.bind)
            }

        case .preparedStatement(let name, let rowDescription):
            return self.avoidingStateMachineCoW { state -> Action in
                switch rowDescription {
                case .some(let rowDescription):
                    state = .rowDescriptionReceived(queryContext, rowDescription.columns)
                case .none:
                    state = .noDataMessageReceived(queryContext)
                }
                return .sendBindExecuteSync(statementName: name, binds: queryContext.bind)
            }
        }
    }
    
    mutating func parseCompletedReceived() -> Action {
        guard case .parseDescribeBindExecuteSyncSent(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.parseComplete))
        }
        
        return self.avoidingStateMachineCoW { state -> Action in
            state = .parseCompleteReceived(queryContext)
            return .wait
        }
    }
    
    mutating func parameterDescriptionReceived(_ parameterDescription: PSQLBackendMessage.ParameterDescription) -> Action {
        guard case .parseCompleteReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.parameterDescription(parameterDescription)))
        }
        
        return self.avoidingStateMachineCoW { state -> Action in
            state = .parameterDescriptionReceived(queryContext)
            return .wait
        }
    }
    
    mutating func noDataReceived() -> Action {
        guard case .parameterDescriptionReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.noData))
        }
        
        return self.avoidingStateMachineCoW { state -> Action in
            state = .noDataMessageReceived(queryContext)
            return .wait
        }
    }
    
    mutating func rowDescriptionReceived(_ rowDescription: PSQLBackendMessage.RowDescription) -> Action {
        guard case .parameterDescriptionReceived(let queryContext) = self.state else {
            return self.setAndFireError(.unexpectedBackendMessage(.rowDescription(rowDescription)))
        }
        
        return self.avoidingStateMachineCoW { state -> Action in
            // In Postgres extended queries we receive the `rowDescription` before we send the
            // `Bind` message. Well actually it's vice versa, but this is only true since we do
            // pipelining during a query.
            //
            // In the actual protocol description we receive a rowDescription before the Bind
            
            // In Postgres extended queries we always request the response rows to be returned in
            // `.binary` format.
            let columns = rowDescription.columns.map { column -> PSQLBackendMessage.RowDescription.Column in                
                var column = column
                column.format = .binary
                return column
            }
            state = .rowDescriptionReceived(queryContext, columns)
            return .wait
        }
    }
    
    mutating func bindCompleteReceived() -> Action {
        switch self.state {
        case .rowDescriptionReceived(let context, let columns):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .bufferingRows(columns, CircularBuffer(), readOnEmpty: false)
                return .succeedQuery(context, columns: columns)
            }
        case .noDataMessageReceived(let queryContext):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .bindCompleteReceived(queryContext)
                return .wait
            }
        case .initialized,
             .parseDescribeBindExecuteSyncSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .bindCompleteReceived,
             .bufferingRows,
             .waitingForNextRow,
             .commandComplete,
             .error:
            return self.setAndFireError(.unexpectedBackendMessage(.bindComplete))
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func dataRowReceived(_ dataRow: PSQLBackendMessage.DataRow) -> Action {
        switch self.state {
        case .bufferingRows(let columns, var buffer, let readOnEmpty):
            // When receiving a data row, we must ensure that the data row column count
            // matches the previously received row description column count.
            guard dataRow.columns.count == columns.count else {
                return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
            }
            
            return self.avoidingStateMachineCoW { state -> Action in
                let row = dataRow.columns.enumerated().map { (index, buffer) in
                    PSQLData(bytes: buffer, dataType: columns[index].dataType, format: columns[index].format)
                }
                buffer.append(row)
                state = .bufferingRows(columns, buffer, readOnEmpty: readOnEmpty)
                return .wait
            }
            
        case .waitingForNextRow(let columns, let buffer, let promise):
            // When receiving a data row, we must ensure that the data row column count
            // matches the previously received row description column count.
            guard dataRow.columns.count == columns.count else {
                return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
            }
            
            return self.avoidingStateMachineCoW { state -> Action in
                precondition(buffer.isEmpty, "Expected the buffer to be empty")
                let row = dataRow.columns.enumerated().map { (index, buffer) in
                    PSQLData(bytes: buffer, dataType: columns[index].dataType, format: columns[index].format)
                }
                
                state = .bufferingRows(columns, buffer, readOnEmpty: false)
                return .forwardRow(row, to: promise)
            }
            
        case .initialized,
             .parseDescribeBindExecuteSyncSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .rowDescriptionReceived,
             .bindCompleteReceived,
             .commandComplete,
             .error:
            return self.setAndFireError(.unexpectedBackendMessage(.dataRow(dataRow)))
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func commandCompletedReceived(_ commandTag: String) -> Action {
        switch self.state {
        case .bindCompleteReceived(let context):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .commandComplete(commandTag: commandTag)
                return .succeedQueryNoRowsComming(context, commandTag: commandTag)
            }
            
        case .bufferingRows(_, let buffer, let readOnEmpty):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .commandComplete(commandTag: commandTag)
                return .forwardStreamCompletedToCurrentQuery(buffer, commandTag: commandTag, read: readOnEmpty)
            }
            
        case .waitingForNextRow(_, let buffer, let promise):
            return self.avoidingStateMachineCoW { state -> Action in
                precondition(buffer.isEmpty, "Expected the buffer to be empty")
                state = .commandComplete(commandTag: commandTag)
                return .forwardCommandComplete(buffer, commandTag: commandTag, to: promise)
            }
        
        case .initialized,
             .parseDescribeBindExecuteSyncSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .rowDescriptionReceived,
             .commandComplete,
             .error:
            return self.setAndFireError(.unexpectedBackendMessage(.commandComplete(commandTag)))
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func emptyQueryResponseReceived() -> Action {
        preconditionFailure("Unimplemented")
    }
    
    mutating func errorReceived(_ errorMessage: PSQLBackendMessage.ErrorResponse) -> Action {
        let error = PSQLError.server(errorMessage)
        switch self.state {
        case .initialized:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
        case .parseDescribeBindExecuteSyncSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .bindCompleteReceived:
            return self.setAndFireError(error)
        case .rowDescriptionReceived, .noDataMessageReceived:
            return self.setAndFireError(error)
        case .bufferingRows:
            return self.setAndFireError(error)
        case .waitingForNextRow:
            return self.setAndFireError(error)
        case .commandComplete:
            return self.setAndFireError(.unexpectedBackendMessage(.error(errorMessage)))
        case .error:
            preconditionFailure("""
                This state must not be reached. If the query `.isComplete`, the
                ConnectionStateMachine must not send any further events to the substate machine.
                """)
            
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func noticeReceived(_ notice: PSQLBackendMessage.NoticeResponse) -> Action {
        //self.queryObject.noticeReceived(notice)
        return .wait
    }
    
    mutating func errorHappened(_ error: PSQLError) -> Action {
        return self.setAndFireError(error)
    }
            
    // MARK: Customer Actions
    
    mutating func consumeNextRow(promise: EventLoopPromise<StateMachineStreamNextResult>) -> Action {
        switch self.state {
        case .waitingForNextRow:
            preconditionFailure("Too greedy. `consumeNextRow()` only needs to be called once.")
            
        case .bufferingRows(let columns, var buffer, let readOnEmpty):
            return self.avoidingStateMachineCoW { state -> Action in
                guard let row = buffer.popFirst() else {
                    state = .waitingForNextRow(columns, buffer, promise)
                    return readOnEmpty ? .read : .wait
                }
                
                state = .bufferingRows(columns, buffer, readOnEmpty: readOnEmpty)
                return .forwardRow(row, to: promise)
            }

        case .initialized,
             .parseDescribeBindExecuteSyncSent,
             .parseCompleteReceived,
             .parameterDescriptionReceived,
             .noDataMessageReceived,
             .rowDescriptionReceived,
             .bindCompleteReceived:
            preconditionFailure("Requested to consume next row without anything going on.")
            
        case .commandComplete, .error:
            preconditionFailure("The stream is already closed or in a failure state; rows can not be consumed at this time.")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    // MARK: Channel actions
    
    mutating func readEventCaught() -> Action {
        switch self.state {
        case .parseDescribeBindExecuteSyncSent:
            return .read
        case .parseCompleteReceived:
            return .read
        case .parameterDescriptionReceived:
            return .read
        case .noDataMessageReceived:
            return .read
        case .rowDescriptionReceived:
            return .read
        case .bindCompleteReceived:
            return .read
        case .bufferingRows(let columns, let buffer, _):
            return self.avoidingStateMachineCoW { state -> Action in
                state = .bufferingRows(columns, buffer, readOnEmpty: true)
                return .wait
            }
        case .waitingForNextRow:
            // we are in the stream and the consumer has already asked us for more rows,
            // therefore we need to read!
            return .read
        case .initialized,
             .commandComplete,
             .error:
            // we already have the complete stream received, now we are waiting for a
            // `readyForQuery` package. To receive this we need to read!
            return .read
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    // MARK: Private Methods
    
    private mutating func setAndFireError(_ error: PSQLError) -> Action {
        switch self.state {
        case .initialized(let context),
             .parseDescribeBindExecuteSyncSent(let context),
             .parseCompleteReceived(let context),
             .parameterDescriptionReceived(let context),
             .rowDescriptionReceived(let context, _),
             .noDataMessageReceived(let context),
             .bindCompleteReceived(let context):
            self.state = .error(error)
            return .failQuery(context, with: error)
        case .bufferingRows(_, _, readOnEmpty: let readOnEmpty):
            self.state = .error(error)
            return .forwardStreamErrorToCurrentQuery(error, read: readOnEmpty)
        case .waitingForNextRow(_, _, let promise):
            self.state = .error(error)
            return .forwardStreamError(error, to: promise)
        case .commandComplete, .error:
            preconditionFailure("""
                This state must not be reached. If the query `.isComplete`, the
                ConnectionStateMachine must not send any further events to the substate machine.
                """)
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    var isComplete: Bool {
        switch self.state {
        case .commandComplete,
             .error:
            return true
        default:
            return false
        }
    }
}

extension ExtendedQueryStateMachine {
    /// So, uh...this function needs some explaining.
    ///
    /// While the state machine logic above is great, there is a downside to having all of the state machine data in
    /// associated data on enumerations: any modification of that data will trigger copy on write for heap-allocated
    /// data. That means that for _every operation on the state machine_ we will CoW our underlying state, which is
    /// not good.
    ///
    /// The way we can avoid this is by using this helper function. It will temporarily set state to a value with no
    /// associated data, before attempting the body of the function. It will also verify that the state machine never
    /// remains in this bad state.
    ///
    /// A key note here is that all callers must ensure that they return to a good state before they exit.
    ///
    /// Sadly, because it's generic and has a closure, we need to force it to be inlined at all call sites, which is
    /// not ideal.
    @inline(__always)
    private mutating func avoidingStateMachineCoW<ReturnType>(_ body: (inout State) -> ReturnType) -> ReturnType {
        self.state = .modifying
        defer {
            assert(!self.isModifying)
        }

        return body(&self.state)
    }

    private var isModifying: Bool {
        if case .modifying = self.state {
            return true
        } else {
            return false
        }
    }
}
