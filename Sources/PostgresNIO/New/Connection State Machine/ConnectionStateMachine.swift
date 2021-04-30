
struct ConnectionStateMachine {
    
    typealias TransactionState = PSQLBackendMessage.TransactionState
    
    struct ConnectionContext {
        let processID: Int32
        let secretKey: Int32
        
        var parameters: [String: String]
        var transactionState: TransactionState
    }
    
    struct BackendKeyData {
        let processID: Int32
        let secretKey: Int32
    }
    
    enum State {
        case initialized
        case sslRequestSent
        case sslNegotiated
        case sslHandlerAdded
        case waitingToStartAuthentication
        case authenticating(AuthenticationStateMachine)
        case authenticated(BackendKeyData?, [String: String])
        
        case readyForQuery(ConnectionContext)
        case extendedQuery(ExtendedQueryStateMachine, ConnectionContext)
        case prepareStatement(PrepareStatementStateMachine, ConnectionContext)
        case closeCommand(CloseStateMachine, ConnectionContext)
        
        case error(PSQLError)
        case closing
        case closed
        
        case modifying
    }
    
    enum QuiescingState {
        case notQuiescing
        case quiescing(closePromise: EventLoopPromise<Void>?)
    }
    
    enum ConnectionAction {
        
        struct CleanUpContext {
            enum Action {
                case close
                case fireChannelInactive
            }
            
            let action: Action
            
            /// Tasks to fail with the error
            let tasks: [PSQLTask]
            
            let error: PSQLError
            
            let closePromise: EventLoopPromise<Void>?
        }
        
        case read
        case wait
        case sendSSLRequest
        case establishSSLConnection
        case provideAuthenticationContext
        case forwardNotificationToListeners(PSQLBackendMessage.NotificationResponse)
        case fireEventReadyForQuery
        case fireChannelInactive
        /// Close the connection by sending a `Terminate` message and then closing the connection. This is for clean shutdowns.
        case closeConnection(EventLoopPromise<Void>?)
        
        /// Close connection because of an error state. Fail all tasks with the provided error.
        case closeConnectionAndCleanup(CleanUpContext)
        
        // Auth Actions
        case sendStartupMessage(AuthContext)
        case sendPasswordMessage(PasswordAuthencationMode, AuthContext)
        case sendSaslInitialResponse(name: String, initialResponse: [UInt8])
        case sendSaslResponse([UInt8])
        
        // Connection Actions
        
        // --- general actions
        case sendParseDescribeBindExecuteSync(query: String, binds: [PSQLEncodable])
        case sendBindExecuteSync(statementName: String, binds: [PSQLEncodable])
        case failQuery(ExtendedQueryContext, with: PSQLError, cleanupContext: CleanUpContext?)
        case succeedQuery(ExtendedQueryContext, columns: [PSQLBackendMessage.RowDescription.Column])
        case succeedQueryNoRowsComming(ExtendedQueryContext, commandTag: String)
        
        // --- streaming actions
        // actions if query has requested next row but we are waiting for backend
        case forwardRows(CircularBuffer<PSQLBackendMessage.DataRow>)
        case forwardStreamComplete(CircularBuffer<PSQLBackendMessage.DataRow>, commandTag: String, read: Bool)
        case forwardStreamError(PSQLError, read: Bool, cleanupContext: CleanUpContext?)
        
        // Prepare statement actions
        case sendParseDescribeSync(name: String, query: String)
        case succeedPreparedStatementCreation(PrepareStatementContext, with: PSQLBackendMessage.RowDescription?)
        case failPreparedStatementCreation(PrepareStatementContext, with: PSQLError, cleanupContext: CleanUpContext?)
        
        // Close actions
        case sendCloseSync(CloseTarget)
        case succeedClose(CloseCommandContext)
        case failClose(CloseCommandContext, with: PSQLError, cleanupContext: CleanUpContext?)
    }
    
    private var state: State
    private var taskQueue = CircularBuffer<PSQLTask>()
    private var quiescingState: QuiescingState = .notQuiescing
    
    init() {
        self.state = .initialized
    }
    
    #if DEBUG
    /// for testing purposes only
    init(_ state: State) {
        self.state = state
    }
    #endif
    
    mutating func connected(requireTLS: Bool) -> ConnectionAction {
        guard case .initialized = self.state else {
            preconditionFailure("Unexpected state")
        }

        if requireTLS {
            self.state = .sslRequestSent
            return .sendSSLRequest
        }
            
        self.state = .waitingToStartAuthentication
        return .provideAuthenticationContext
    }
    
    mutating func provideAuthenticationContext(_ authContext: AuthContext) -> ConnectionAction {
        self.startAuthentication(authContext)
    }
    
    mutating func close(_ promise: EventLoopPromise<Void>?) -> ConnectionAction {
        switch self.state {
        case .closing, .closed, .error:
            // we are already closed, but sometimes an upstream handler might want to close the
            // connection, though it has already been closed by the remote. Typical race condition.
            return .closeConnection(promise)
        case .readyForQuery:
            precondition(self.taskQueue.isEmpty, """
                The state should only be .readyForQuery if there are no more tasks in the queue
                """)
            self.state = .closing
            return .closeConnection(promise)
        default:
            switch self.quiescingState {
            case .notQuiescing:
                self.quiescingState = .quiescing(closePromise: promise)
            case .quiescing(.some(let closePromise)):
                closePromise.futureResult.cascade(to: promise)
            case .quiescing(.none):
                self.quiescingState = .quiescing(closePromise: promise)
            }
            return .wait
        }
    }
    
    mutating func closed() -> ConnectionAction {
        switch self.state {
        case .initialized:
            preconditionFailure("How can a connection be closed, if it was never connected.")
        case .closed:
            preconditionFailure("How can a connection be closed, if it is already closed.")
        case .authenticated,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .readyForQuery,
             .extendedQuery,
             .prepareStatement,
             .closeCommand:
            return self.errorHappened(.uncleanShutdown)
        case .error, .closing:
            self.state = .closed
            self.quiescingState = .notQuiescing
            return .fireChannelInactive
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func sslSupportedReceived() -> ConnectionAction {
        switch self.state {
        case .sslRequestSent:
            self.state = .sslNegotiated
            return .establishSSLConnection
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.sslSupported))
        }
    }
    
    mutating func sslUnsupportedReceived() -> ConnectionAction {
        switch self.state {
        case .sslRequestSent:
            return self.closeConnectionAndCleanup(.sslUnsupported)
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.sslSupported))
        }
    }
    
    mutating func sslHandlerAdded() -> ConnectionAction {
        guard case .sslNegotiated = self.state else {
            preconditionFailure("Can only add a ssl handler after negotiation")
        }
        
        self.state = .sslHandlerAdded
        return .wait
    }
    
    mutating func sslEstablished() -> ConnectionAction {
        guard case .sslHandlerAdded = self.state else {
            preconditionFailure("Can only establish a ssl connection after adding a ssl handler")
        }
        
        self.state = .waitingToStartAuthentication
        return .provideAuthenticationContext
    }
    
    mutating func authenticationMessageReceived(_ message: PSQLBackendMessage.Authentication) -> ConnectionAction {
        guard case .authenticating(var authState) = self.state else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.authentication(message)))
        }
        
        return self.avoidingStateMachineCoW { machine in
            let action = authState.authenticationMessageReceived(message)
            machine.state = .authenticating(authState)
            return machine.modify(with: action)
        }
    }
    
    mutating func backendKeyDataReceived(_ keyData: PSQLBackendMessage.BackendKeyData) -> ConnectionAction {
        guard case .authenticated(_, let parameters) = self.state else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.backendKeyData(keyData)))
        }
        
        let keyData = BackendKeyData(
            processID: keyData.processID,
            secretKey: keyData.secretKey)
        
        self.state = .authenticated(keyData, parameters)
        return .wait
    }
    
    mutating func parameterStatusReceived(_ status: PSQLBackendMessage.ParameterStatus) -> ConnectionAction {
        switch self.state {
        case .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticating,
             .closing:
            self.state = .error(.unexpectedBackendMessage(.parameterStatus(status)))
            return .wait
        case .authenticated(let keyData, var parameters):
            return self.avoidingStateMachineCoW { machine in
                parameters[status.parameter] = status.value
                machine.state = .authenticated(keyData, parameters)
                return .wait
            }
        case .readyForQuery(var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .readyForQuery(connectionContext)
                return .wait
            }
        case .extendedQuery(let query, var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .extendedQuery(query, connectionContext)
                return .wait
            }
        case .prepareStatement(let prepareState, var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .prepareStatement(prepareState, connectionContext)
                return .wait
            }
        case .closeCommand(let closeState, var connectionContext):
            return self.avoidingStateMachineCoW { machine in
                connectionContext.parameters[status.parameter] = status.value
                machine.state = .closeCommand(closeState, connectionContext)
                return .wait
            }
        case .error(_):
            return .wait
        case .initialized,
             .closed:
            preconditionFailure("We shouldn't receive messages if we are not connected")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func errorReceived(_ errorMessage: PSQLBackendMessage.ErrorResponse) -> ConnectionAction {
        switch self.state {
        case .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticated,
             .readyForQuery,
             .error:
            return self.closeConnectionAndCleanup(.server(errorMessage))
        case .authenticating(var authState):
            if authState.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = authState.errorReceived(errorMessage)
                machine.state = .authenticating(authState)
                return machine.modify(with: action)
            }
        case .closeCommand(var closeStateMachine, let connectionContext):
            if closeStateMachine.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = closeStateMachine.errorReceived(errorMessage)
                machine.state = .closeCommand(closeStateMachine, connectionContext)
                return machine.modify(with: action)
            }
        case .extendedQuery(var extendedQueryState, let connectionContext):
            if extendedQueryState.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = extendedQueryState.errorReceived(errorMessage)
                machine.state = .extendedQuery(extendedQueryState, connectionContext)
                return machine.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext):
            if preparedState.isComplete {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.error(errorMessage)))
            }
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = preparedState.errorReceived(errorMessage)
                machine.state = .prepareStatement(preparedState, connectionContext)
                return machine.modify(with: action)
            }
        case .closing:
            // If the state machine is in state `.closing`, the connection shutdown was initiated
            // by the client. This means a `TERMINATE` message has already been sent and the
            // connection close was passed on to the channel. Therefore we await a channelInactive
            // as the next event.
            // Since a connection close was already issued, we should keep cool and just wait.
            return .wait
        case .initialized, .closed:
            preconditionFailure("We should not receive server errors if we are not connected")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func errorHappened(_ error: PSQLError) -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticated,
             .readyForQuery:
            return self.closeConnectionAndCleanup(error)
        case .authenticating(var authState):
            let action = authState.errorHappened(error)
            return self.modify(with: action)
        case .extendedQuery(var queryState, _):
            if queryState.isComplete {
                return self.closeConnectionAndCleanup(error)
            } else {
                let action = queryState.errorHappened(error)
                return self.modify(with: action)
            }
        case .prepareStatement(var prepareState, _):
            if prepareState.isComplete {
                return self.closeConnectionAndCleanup(error)
            } else {
                let action = prepareState.errorHappened(error)
                return self.modify(with: action)
            }
        case .closeCommand(var closeState, _):
            if closeState.isComplete {
                return self.closeConnectionAndCleanup(error)
            } else {
                let action = closeState.errorHappened(error)
                return self.modify(with: action)
            }
        case .error:
            return .wait
        case .closing:
            // If the state machine is in state `.closing`, the connection shutdown was initiated
            // by the client. This means a `TERMINATE` message has already been sent and the
            // connection close was passed on to the channel. Therefore we await a channelInactive
            // as the next event.
            // For some reason Azure Postgres does not end ssl cleanly when terminating the
            // connection. More documentation can be found in the issue:
            // https://github.com/vapor/postgres-nio/issues/150
            // Since a connection close was already issued, we should keep cool and just wait.
            return .wait
        case .closed:
            return self.closeConnectionAndCleanup(error)
        
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    mutating func noticeReceived(_ notice: PSQLBackendMessage.NoticeResponse) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = extendedQuery.noticeReceived(notice)
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return .wait
        }
    }
    
    mutating func notificationReceived(_ notification: PSQLBackendMessage.NotificationResponse) -> ConnectionAction {
        return .forwardNotificationToListeners(notification)
    }
    
    mutating func readyForQueryReceived(_ transactionState: PSQLBackendMessage.TransactionState) -> ConnectionAction {
        switch self.state {
        case .authenticated(let backendKeyData, let parameters):
            guard let keyData = backendKeyData else {
                // `backendKeyData` must have been received, before receiving the first `readyForQuery`
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            let connectionContext = ConnectionContext(
                processID: keyData.processID,
                secretKey: keyData.secretKey,
                parameters: parameters,
                transactionState: transactionState)
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        case .extendedQuery(let extendedQuery, var connectionContext):
            guard extendedQuery.isComplete else {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        case .prepareStatement(let preparedStateMachine, var connectionContext):
            guard preparedStateMachine.isComplete else {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
        
        case .closeCommand(let closeStateMachine, var connectionContext):
            guard closeStateMachine.isComplete else {
                return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
            }
            
            connectionContext.transactionState = transactionState
            
            self.state = .readyForQuery(connectionContext)
            return self.executeNextQueryFromQueue()
            
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.readyForQuery(transactionState)))
        }
    }
    
    mutating func enqueue(task: PSQLTask) -> ConnectionAction {
        // check if we are quiescing. if so fail task immidiatly
        if case .quiescing = self.quiescingState {
            switch task {
            case .extendedQuery(let queryContext):
                return .failQuery(queryContext, with: .connectionQuiescing, cleanupContext: nil)
            case .preparedStatement(let prepareContext):
                return .failPreparedStatementCreation(prepareContext, with: .connectionQuiescing, cleanupContext: nil)
            case .closeCommand(let closeContext):
                return .failClose(closeContext, with: .connectionQuiescing, cleanupContext: nil)
            }
        }

        switch self.state {
        case .readyForQuery:
            return self.executeTask(task)
        case .closed:
            switch task {
            case .extendedQuery(let queryContext):
                return .failQuery(queryContext, with: .connectionClosed, cleanupContext: nil)
            case .preparedStatement(let prepareContext):
                return .failPreparedStatementCreation(prepareContext, with: .connectionClosed, cleanupContext: nil)
            case .closeCommand(let closeContext):
                return .failClose(closeContext, with: .connectionClosed, cleanupContext: nil)
            }
        default:
            self.taskQueue.append(task)
            return .wait
        }
    }
    
    mutating func readEventCaught() -> ConnectionAction {
        switch self.state {
        case .initialized:
            preconditionFailure("Received a read event on a connection that was never opened.")
        case .sslRequestSent:
            return .read
        case .sslNegotiated:
            return .read
        case .sslHandlerAdded:
            return .read
        case .waitingToStartAuthentication:
            return .read
        case .authenticating:
            return .read
        case .authenticated:
            return .read
        case .readyForQuery:
            return .read
        case .extendedQuery(var extendedQuery, let connectionContext):
            return self.avoidingStateMachineCoW { machine in
                let action = extendedQuery.readEventCaught()
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        case .prepareStatement(var preparedStatement, let connectionContext):
            return self.avoidingStateMachineCoW { machine in
                let action = preparedStatement.readEventCaught()
                machine.state = .prepareStatement(preparedStatement, connectionContext)
                return machine.modify(with: action)
            }
        case .closeCommand(var closeState, let connectionContext):
            return self.avoidingStateMachineCoW { machine in
                let action = closeState.readEventCaught()
                machine.state = .closeCommand(closeState, connectionContext)
                return machine.modify(with: action)
            }
        case .error:
            return .read
        case .closing:
            return .read
        case .closed:
            preconditionFailure("How can we receive a read, if the connection is closed")
        case .modifying:
            preconditionFailure("Invalid state")

        }
    }
    
    // MARK: - Running Queries -
    
    mutating func parseCompleteReceived() -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.parseCompletedReceived()
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext) where !preparedState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = preparedState.parseCompletedReceived()
                machine.state = .prepareStatement(preparedState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.parseComplete))
        }
    }
    
    mutating func bindCompleteReceived() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.bindComplete))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.bindCompleteReceived()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func parameterDescriptionReceived(_ description: PSQLBackendMessage.ParameterDescription) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.parameterDescriptionReceived(description)
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext) where !preparedState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = preparedState.parameterDescriptionReceived(description)
                machine.state = .prepareStatement(preparedState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.parameterDescription(description)))
        }
    }
    
    mutating func rowDescriptionReceived(_ description: PSQLBackendMessage.RowDescription) -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.rowDescriptionReceived(description)
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext) where !preparedState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = preparedState.rowDescriptionReceived(description)
                machine.state = .prepareStatement(preparedState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.rowDescription(description)))
        }
    }
    
    mutating func noDataReceived() -> ConnectionAction {
        switch self.state {
        case .extendedQuery(var queryState, let connectionContext) where !queryState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = queryState.noDataReceived()
                machine.state = .extendedQuery(queryState, connectionContext)
                return machine.modify(with: action)
            }
        case .prepareStatement(var preparedState, let connectionContext) where !preparedState.isComplete:
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                let action = preparedState.noDataReceived()
                machine.state = .prepareStatement(preparedState, connectionContext)
                return machine.modify(with: action)
            }
        default:
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.noData))
        }
    }

    mutating func portalSuspendedReceived() -> ConnectionAction {
        self.closeConnectionAndCleanup(.unexpectedBackendMessage(.portalSuspended))
    }
    
    mutating func closeCompletedReceived() -> ConnectionAction {
        guard case .closeCommand(var closeState, let connectionContext) = self.state, !closeState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.closeComplete))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = closeState.closeCompletedReceived()
            machine.state = .closeCommand(closeState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func commandCompletedReceived(_ commandTag: String) -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.commandComplete(commandTag)))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.commandCompletedReceived(commandTag)
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func emptyQueryResponseReceived() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.emptyQueryResponse))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.emptyQueryResponseReceived()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    mutating func dataRowsReceived(_ dataRows: CircularBuffer<PSQLBackendMessage.DataRow>) -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            return self.closeConnectionAndCleanup(.unexpectedBackendMessage(.dataRow(dataRows.first!)))
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.dataRowsReceived(dataRows)
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    // MARK: Consumer
    
    mutating func cancelQueryStream() -> ConnectionAction {
        preconditionFailure("Unimplemented")
    }
    
    mutating func requestQueryRows() -> ConnectionAction {
        guard case .extendedQuery(var queryState, let connectionContext) = self.state, !queryState.isComplete else {
            preconditionFailure("Tried to consume next row, without active query")
        }
        
        return self.avoidingStateMachineCoW { machine -> ConnectionAction in
            let action = queryState.requestQueryRows()
            machine.state = .extendedQuery(queryState, connectionContext)
            return machine.modify(with: action)
        }
    }
    
    // MARK: - Private Methods -
    
    private mutating func startAuthentication(_ authContext: AuthContext) -> ConnectionAction {
        guard case .waitingToStartAuthentication = self.state else {
            preconditionFailure("Can only start authentication after connect or ssl establish")
        }
        
        return self.avoidingStateMachineCoW { machine in
            var authState = AuthenticationStateMachine(authContext: authContext)
            let action = authState.start()
            machine.state = .authenticating(authState)
            return machine.modify(with: action)
        }
    }
    
    private mutating func closeConnectionAndCleanup(_ error: PSQLError) -> ConnectionAction {
        switch self.state {
        case .initialized,
             .sslRequestSent,
             .sslNegotiated,
             .sslHandlerAdded,
             .waitingToStartAuthentication,
             .authenticated,
             .readyForQuery:
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            return .closeConnectionAndCleanup(cleanupContext)

        case .authenticating(var authState):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            
            if authState.isComplete {
                // in case the auth state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }
            
            let action = authState.errorHappened(error)
            guard case .reportAuthenticationError = action else {
                preconditionFailure("Expect to fail auth")
            }
            return .closeConnectionAndCleanup(cleanupContext)
        case .extendedQuery(var queryStateMachine, _):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            
            if queryStateMachine.isComplete {
                // in case the query state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }
            
            switch queryStateMachine.errorHappened(error) {
            case .sendParseDescribeBindExecuteSync,
                 .sendBindExecuteSync,
                 .succeedQuery,
                 .succeedQueryNoRowsComming,
                 .forwardRows,
                 .forwardStreamComplete,
                 .wait,
                 .read:
                preconditionFailure("Expecting only failure actions if an error happened")
            case .failQuery(let queryContext, with: let error):
                return .failQuery(queryContext, with: error, cleanupContext: cleanupContext)
            case .forwardStreamError(let error, let read):
                return .forwardStreamError(error, read: read, cleanupContext: cleanupContext)
            }
        case .prepareStatement(var prepareStateMachine, _):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            
            if prepareStateMachine.isComplete {
                // in case the prepare state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }
            
            switch prepareStateMachine.errorHappened(error) {
            case .sendParseDescribeSync,
                 .succeedPreparedStatementCreation,
                 .read,
                 .wait:
                preconditionFailure("Expecting only failure actions if an error happened")
            case .failPreparedStatementCreation(let preparedStatementContext, with: let error):
                return .failPreparedStatementCreation(preparedStatementContext, with: error, cleanupContext: cleanupContext)
            }
        case .closeCommand(var closeStateMachine, _):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            
            if closeStateMachine.isComplete {
                // in case the close state machine is complete all necessary actions have already
                // been forwarded to the consumer. We can close and cleanup without caring about the
                // substate machine.
                return .closeConnectionAndCleanup(cleanupContext)
            }
            
            switch closeStateMachine.errorHappened(error) {
            case .sendCloseSync,
                 .succeedClose,
                 .read,
                 .wait:
                preconditionFailure("Expecting only failure actions if an error happened")
            case .failClose(let closeCommandContext, with: let error):
                return .failClose(closeCommandContext, with: error, cleanupContext: cleanupContext)
            }
        case .error:
            // TBD: this is an interesting case. why would this case happen?
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            return .closeConnectionAndCleanup(cleanupContext)
            
        case .closing:
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            return .closeConnectionAndCleanup(cleanupContext)
        case .closed:
            preconditionFailure("How can an error occur if the connection is already closed?")
        case .modifying:
            preconditionFailure("Invalid state")
        }
    }
    
    private mutating func executeNextQueryFromQueue() -> ConnectionAction {
        guard case .readyForQuery = self.state else {
            preconditionFailure("Only expected to be invoked, if we are readyToQuery")
        }
        
        if let task = self.taskQueue.popFirst() {
            return self.executeTask(task)
        }
        
        // if we don't have anything left to do and we are quiescing, next we should close
        if case .quiescing(let promise) = self.quiescingState {
            self.state = .closing
            return .closeConnection(promise)
        }
        
        return .fireEventReadyForQuery
    }
    
    private mutating func executeTask(_ task: PSQLTask) -> ConnectionAction {
        guard case .readyForQuery(let connectionContext) = self.state else {
            preconditionFailure("Only expected to be invoked, if we are readyToQuery")
        }
        
        switch task {
        case .extendedQuery(let queryContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                var extendedQuery = ExtendedQueryStateMachine(queryContext: queryContext)
                let action = extendedQuery.start()
                machine.state = .extendedQuery(extendedQuery, connectionContext)
                return machine.modify(with: action)
            }
        case .preparedStatement(let prepareContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                var prepareStatement = PrepareStatementStateMachine(createContext: prepareContext)
                let action = prepareStatement.start()
                machine.state = .prepareStatement(prepareStatement, connectionContext)
                return machine.modify(with: action)
            }
        case .closeCommand(let closeContext):
            return self.avoidingStateMachineCoW { machine -> ConnectionAction in
                var closeStateMachine = CloseStateMachine(closeContext: closeContext)
                let action = closeStateMachine.start()
                machine.state = .closeCommand(closeStateMachine, connectionContext)
                return machine.modify(with: action)
            }
        }
    }
    
    struct Configuration {
        let requireTLS: Bool
    }
}

// MARK: CoW helpers

extension ConnectionStateMachine {
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
    private mutating func avoidingStateMachineCoW<ReturnType>(_ body: (inout ConnectionStateMachine) -> ReturnType) -> ReturnType {
        self.state = .modifying
        defer {
            assert(!self.isModifying)
        }

        return body(&self)
    }

    private var isModifying: Bool {
        if case .modifying = self.state {
            return true
        } else {
            return false
        }
    }
}

extension ConnectionStateMachine {
    func shouldCloseConnection(reason error: PSQLError) -> Bool {
        switch error.base {
        case .sslUnsupported:
            return true
        case .failedToAddSSLHandler:
            return true
        case .server(let message):
            guard let sqlState = message.fields[.sqlState] else {
                // any error message that doesn't have a sql state field, is unexpected by default.
                return true
            }
            
            if sqlState.starts(with: "28") {
                // these are authentication errors
                return true
            }
            
            return false
        case .decoding(_):
            return true
        case .unexpectedBackendMessage(_):
            return true
        case .unsupportedAuthMechanism(_):
            return true
        case .authMechanismRequiresPassword:
            return true
        case .saslError:
            return true
        case .tooManyParameters:
            return true
        case .connectionQuiescing:
            preconditionFailure("Pure client error, that is thrown directly in PSQLConnection")
        case .connectionClosed:
            preconditionFailure("Pure client error, that is thrown directly and should never ")
        case .connectionError:
            return true
        case .casting(_):
            preconditionFailure("Pure client error, that is thrown directly in PSQLRows")
        case .uncleanShutdown:
            return true
        }
    }

    mutating func setErrorAndCreateCleanupContextIfNeeded(_ error: PSQLError) -> ConnectionAction.CleanUpContext? {
        guard self.shouldCloseConnection(reason: error) else {
            return nil
        }
        
        return self.setErrorAndCreateCleanupContext(error)
    }
    
    mutating func setErrorAndCreateCleanupContext(_ error: PSQLError) -> ConnectionAction.CleanUpContext {
        let tasks = Array(self.taskQueue)
        self.taskQueue.removeAll()
        
        var closePromise: EventLoopPromise<Void>? = nil
        if case .quiescing(let promise) = self.quiescingState {
            closePromise = promise
        }
        
        self.state = .error(error)
        
        var action = ConnectionAction.CleanUpContext.Action.close
        if case .uncleanShutdown = error.base {
            action = .fireChannelInactive
        }
        
        return .init(action: action, tasks: tasks, error: error, closePromise: closePromise)
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: ExtendedQueryStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendParseDescribeBindExecuteSync(let query, let binds):
            return .sendParseDescribeBindExecuteSync(query: query, binds: binds)
        case .sendBindExecuteSync(let statementName, let binds):
            return .sendBindExecuteSync(statementName: statementName, binds: binds)
        case .failQuery(let requestContext, with: let error):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .failQuery(requestContext, with: error, cleanupContext: cleanupContext)
        case .succeedQuery(let requestContext, columns: let columns):
            return .succeedQuery(requestContext, columns: columns)
        case .succeedQueryNoRowsComming(let requestContext, let commandTag):
            return .succeedQueryNoRowsComming(requestContext, commandTag: commandTag)
        case .forwardRows(let buffer):
            return .forwardRows(buffer)
        case .forwardStreamComplete(let buffer, let commandTag, let read):
            return .forwardStreamComplete(buffer, commandTag: commandTag, read: read)
        case .forwardStreamError(let error, let read):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .forwardStreamError(error, read: read, cleanupContext: cleanupContext)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: PrepareStatementStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendParseDescribeSync(let name, let query):
            return .sendParseDescribeSync(name: name, query: query)
        case .succeedPreparedStatementCreation(let prepareContext, with: let rowDescription):
            return .succeedPreparedStatementCreation(prepareContext, with: rowDescription)
        case .failPreparedStatementCreation(let prepareContext, with: let error):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .failPreparedStatementCreation(prepareContext, with: error, cleanupContext: cleanupContext)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: AuthenticationStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendStartupMessage(let authContext):
            return .sendStartupMessage(authContext)
        case .sendPassword(let mode, let authContext):
            return .sendPasswordMessage(mode, authContext)
        case .sendSaslInitialResponse(let name, let initialResponse):
            return .sendSaslInitialResponse(name: name, initialResponse: initialResponse)
        case .sendSaslResponse(let bytes):
            return .sendSaslResponse(bytes)
        case .authenticated:
            self.state = .authenticated(nil, [:])
            return .wait
        case .wait:
            return .wait
        case .reportAuthenticationError(let error):
            let cleanupContext = self.setErrorAndCreateCleanupContext(error)
            return .closeConnectionAndCleanup(cleanupContext)
        }
    }
}

extension ConnectionStateMachine {
    mutating func modify(with action: CloseStateMachine.Action) -> ConnectionStateMachine.ConnectionAction {
        switch action {
        case .sendCloseSync(let sendClose):
            return .sendCloseSync(sendClose)
        case .succeedClose(let closeContext):
            return .succeedClose(closeContext)
        case .failClose(let closeContext, with: let error):
            let cleanupContext = self.setErrorAndCreateCleanupContextIfNeeded(error)
            return .failClose(closeContext, with: error, cleanupContext: cleanupContext)
        case .read:
            return .read
        case .wait:
            return .wait
        }
    }
}

struct SendPrepareStatement {
    let name: String
    let query: String
}

struct AuthContext: Equatable, CustomDebugStringConvertible {
    let username: String
    let password: String?
    let database: String?
    
    var debugDescription: String {
        """
        AuthContext(username: \(String(reflecting: self.username)), \
        password: \(self.password != nil ? "********" : "nil"), \
        database: \(self.database != nil ? String(reflecting: self.database!) : "nil"))
        """
    }
}

enum PasswordAuthencationMode: Equatable {
    case cleartext
    case md5(salt: (UInt8, UInt8, UInt8, UInt8))
    
    static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.cleartext, .cleartext):
            return true
        case (.md5(let lhs), .md5(let rhs)):
            return lhs == rhs
        default:
            return false
        }
    }
}

extension ConnectionStateMachine.State: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .initialized:
            return ".initialized"
        case .sslRequestSent:
            return ".sslRequestSent"
        case .sslNegotiated:
            return ".sslNegotiated"
        case .sslHandlerAdded:
            return ".sslHandlerAdded"
        case .waitingToStartAuthentication:
            return ".waitingToStartAuthentication"
        case .authenticating(let authStateMachine):
            return ".authenticating(\(String(reflecting: authStateMachine)))"
        case .authenticated(let backendKeyData, let parameters):
            return ".authenticated(\(String(reflecting: backendKeyData)), \(String(reflecting: parameters)))"
        case .readyForQuery(let connectionContext):
            return ".readyForQuery(connectionContext: \(String(reflecting: connectionContext)))"
        case .extendedQuery(let subStateMachine, let connectionContext):
            return ".extendedQuery(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .prepareStatement(let subStateMachine, let connectionContext):
            return ".prepareStatement(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .closeCommand(let subStateMachine, let connectionContext):
            return ".closeCommand(\(String(reflecting: subStateMachine)), connectionContext: \(String(reflecting: connectionContext)))"
        case .error(let error):
            return ".error(\(String(reflecting: error)))"
        case .closing:
            return ".closing"
        case .closed:
            return ".closed"
        case .modifying:
            return ".modifying"
        }
    }
}

extension ConnectionStateMachine.ConnectionContext: CustomDebugStringConvertible {
    var debugDescription: String {
        """
        (processID: \(self.processID), \
        secretKey: \(self.secretKey), \
        parameters: \(String(reflecting: self.parameters)))
        """
    }
}

extension ConnectionStateMachine.QuiescingState: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .notQuiescing:
            return ".notQuiescing"
        case .quiescing(let closePromise):
            return ".quiescing(\(closePromise != nil ? "\(closePromise!)" : "nil"))"
        }
    }
}

