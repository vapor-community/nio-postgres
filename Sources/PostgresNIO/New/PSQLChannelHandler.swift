import NIO
import NIOTLS
import Crypto
import Logging

protocol PSQLChannelHandlerNotificationDelegate: AnyObject {
    func notificationReceived(_: PSQLBackendMessage.NotificationResponse)
}

final class PSQLChannelHandler: ChannelDuplexHandler {
    typealias InboundIn = PSQLOptimizedBackendMessage
    typealias OutboundIn = PSQLTask
    typealias OutboundOut = PSQLFrontendMessage

    private let logger: Logger
    private var state: ConnectionStateMachine {
        didSet {
            self.logger.trace("Connection state changed", metadata: [.connectionState: "\(self.state)"])
        }
    }
    
    /// A `ChannelHandlerContext` to be used for non channel related events. (for example: More rows needed).
    ///
    /// The context is captured in `handlerAdded` and released` in `handlerRemoved`
    private var handlerContext: ChannelHandlerContext!
    private var rowStream: PSQLRowBatchStream?
    private let authentificationConfiguration: PSQLConnection.Configuration.Authentication?
    private let enableSSLCallback: ((Channel) -> EventLoopFuture<Void>)?
    
    /// this delegate should only be accessed on the connections `EventLoop`
    weak var notificationDelegate: PSQLChannelHandlerNotificationDelegate?
    
    init(authentification: PSQLConnection.Configuration.Authentication?,
         logger: Logger,
         enableSSLCallback: ((Channel) -> EventLoopFuture<Void>)? = nil)
    {
        self.state = ConnectionStateMachine()
        self.authentificationConfiguration = authentification
        self.enableSSLCallback = enableSSLCallback
        self.logger = logger
    }
    
    #if DEBUG
    /// for testing purposes only
    init(authentification: PSQLConnection.Configuration.Authentication?,
         state: ConnectionStateMachine = .init(.initialized),
         logger: Logger = .psqlNoOpLogger,
         enableSSLCallback: ((Channel) -> EventLoopFuture<Void>)? = nil)
    {
        self.state = state
        self.authentificationConfiguration = authentification
        self.enableSSLCallback = enableSSLCallback
        self.logger = logger
    }
    #endif
    
    // MARK: Handler lifecycle
    
    func handlerAdded(context: ChannelHandlerContext) {
        self.handlerContext = context
        if context.channel.isActive {
            self.connected(context: context)
        }
    }
    
    func handlerRemoved(context: ChannelHandlerContext) {
        self.handlerContext = nil
    }
    
    // MARK: Channel handler incoming
    
    func channelActive(context: ChannelHandlerContext) {
        // `fireChannelActive` needs to be called BEFORE we set the state machine to connected,
        // since we want to make sure that upstream handlers know about the active connection before
        // it receives a 
        context.fireChannelActive()
        
        self.connected(context: context)
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        self.logger.trace("Channel inactive.")
        let action = self.state.closed()
        self.run(action, with: context)
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.debug("Channel error caught.", metadata: [.error: "\(error)"])
        let action = self.state.errorHappened(.channel(underlying: error))
        self.run(action, with: context)
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let incomingMessage = self.unwrapInboundIn(data)
        
        self.logger.trace("Backend message received", metadata: [.message: "\(incomingMessage)"])
        
        let action: ConnectionStateMachine.ConnectionAction
        
        switch incomingMessage {
        case .pure(.authentication(let authentication)):
            action = self.state.authenticationMessageReceived(authentication)
        case .pure(.backendKeyData(let keyData)):
            action = self.state.backendKeyDataReceived(keyData)
        case .pure(.bindComplete):
            action = self.state.bindCompleteReceived()
        case .pure(.closeComplete):
            action = self.state.closeCompletedReceived()
        case .pure(.commandComplete(let commandTag)):
            action = self.state.commandCompletedReceived(commandTag)
        case .pure(.dataRow):
            preconditionFailure("Expected dataRow are only transferred in an optimized format")
        case .dataRows(let dataRows):
            action = self.state.dataRowsReceived(dataRows)
        case .pure(.emptyQueryResponse):
            action = self.state.emptyQueryResponseReceived()
        case .pure(.error(let errorResponse)):
            action = self.state.errorReceived(errorResponse)
        case .pure(.noData):
            action = self.state.noDataReceived()
        case .pure(.notice(let noticeResponse)):
            action = self.state.noticeReceived(noticeResponse)
        case .pure(.notification(let notification)):
            action = self.state.notificationReceived(notification)
        case .pure(.parameterDescription(let parameterDescription)):
            action = self.state.parameterDescriptionReceived(parameterDescription)
        case .pure(.parameterStatus(let parameterStatus)):
            action = self.state.parameterStatusReceived(parameterStatus)
        case .pure(.parseComplete):
            action = self.state.parseCompleteReceived()
        case .pure(.portalSuspended):
            action = self.state.portalSuspendedReceived()
        case .pure(.readyForQuery(let transactionState)):
            action = self.state.readyForQueryReceived(transactionState)
        case .pure(.rowDescription(let rowDescription)):
            action = self.state.rowDescriptionReceived(rowDescription)
        case .pure(.sslSupported):
            action = self.state.sslSupportedReceived()
        case .pure(.sslUnsupported):
            action = self.state.sslUnsupportedReceived()
        }
        
        self.run(action, with: context)
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        self.logger.trace("User inbound event received", metadata: [
            .userEvent: "\(event)"
        ])
        
        switch event {
        case TLSUserEvent.handshakeCompleted:
            let action = self.state.sslEstablished()
            self.run(action, with: context)
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }
    
    // MARK: Channel handler outgoing
    
    func read(context: ChannelHandlerContext) {
        self.logger.trace("Channel read event received")
        let action = self.state.readEventCaught()
        self.run(action, with: context)
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let task = self.unwrapOutboundIn(data)
        let action = self.state.enqueue(task: task)
        self.run(action, with: context)
    }
    
    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        self.logger.trace("Close triggered by upstream.")
        guard mode == .all else {
            // TODO: Support also other modes ?
            promise?.fail(ChannelError.operationUnsupported)
            return
        }

        let action = self.state.close(promise)
        self.run(action, with: context)
    }
    
    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        self.logger.trace("User outbound event received", metadata: [.userEvent: "\(event)"])
        
        switch event {
        case PSQLOutgoingEvent.authenticate(let authContext):
            let action = self.state.provideAuthenticationContext(authContext)
            self.run(action, with: context)
        default:
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }

    // MARK: Channel handler actions
    
    func run(_ action: ConnectionStateMachine.ConnectionAction, with context: ChannelHandlerContext) {
        self.logger.trace("Run action", metadata: [.connectionAction: "\(action)"])
        
        switch action {
        case .establishSSLConnection:
            self.establishSSLConnection(context: context)
        case .read:
            context.read()
        case .wait:
            break
        case .sendStartupMessage(let authContext):
            context.writeAndFlush(.startup(.versionThree(parameters: authContext.toStartupParameters())), promise: nil)
        case .sendSSLRequest:
            context.writeAndFlush(.sslRequest(.init()), promise: nil)
        case .sendPasswordMessage(let mode, let authContext):
            self.sendPasswordMessage(mode: mode, authContext: authContext, context: context)
        case .sendSaslInitialResponse(let name, let initialResponse):
            context.writeAndFlush(.saslInitialResponse(.init(saslMechanism: name, initialData: initialResponse)))
        case .sendSaslResponse(let bytes):
            context.writeAndFlush(.saslResponse(.init(data: bytes)))
        case .closeConnectionAndCleanup(let cleanupContext):
            self.closeConnectionAndCleanup(cleanupContext, context: context)
        case .fireChannelInactive:
            context.fireChannelInactive()
        case .sendParseDescribeSync(let name, let query):
            self.sendParseDecribeAndSyncMessage(statementName: name, query: query, context: context)
        case .sendBindExecuteSync(let statementName, let binds):
            self.sendBindExecuteAndSyncMessage(statementName: statementName, binds: binds, context: context)
        case .sendParseDescribeBindExecuteSync(let query, let binds):
            self.sendParseDescribeBindExecuteAndSyncMessage(query: query, binds: binds, context: context)
        case .succeedQuery(let queryContext, columns: let columns):
            self.succeedQueryWithRowStream(queryContext, columns: columns, context: context)
        case .succeedQueryNoRowsComming(let queryContext, let commandTag):
            self.succeedQueryWithoutRowStream(queryContext, commandTag: commandTag, context: context)
        case .failQuery(let queryContext, with: let error, let cleanupContext):
            queryContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .forwardRows(let rows):
            self.rowStream!.receive(rows)
            
        case .forwardStreamComplete(let buffer, let commandTag, let read):
            if buffer.count > 0 {
                self.rowStream!.receive(buffer)
            }
            self.rowStream!.receive(completion: .success(commandTag))
            self.rowStream = nil
            if read {
                context.read()
            }
        case .forwardStreamError(let error, let read, let cleanupContext):
            self.rowStream!.receive(completion: .failure(error))
            self.rowStream = nil
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            } else if read {
                context.read()
            }
        case .provideAuthenticationContext:
            context.fireUserInboundEventTriggered(PSQLEvent.readyForStartup)
            
            if let authentication = self.authentificationConfiguration {
                let authContext = AuthContext(
                    username: authentication.username,
                    password: authentication.password,
                    database: authentication.database
                )
                let action = self.state.provideAuthenticationContext(authContext)
                return self.run(action, with: context)
            }
        case .fireEventReadyForQuery:
            context.fireUserInboundEventTriggered(PSQLEvent.readyForQuery)
        case .closeConnection(let promise):
            if context.channel.isActive {
                // The normal, graceful termination procedure is that the frontend sends a Terminate
                // message and immediately closes the connection. On receipt of this message, the
                // backend closes the connection and terminates.
                context.write(.terminate, promise: nil)
            }
            context.close(mode: .all, promise: promise)
        case .succeedPreparedStatementCreation(let preparedContext, with: let rowDescription):
            preparedContext.promise.succeed(rowDescription)
        case .failPreparedStatementCreation(let preparedContext, with: let error, let cleanupContext):
            preparedContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .sendCloseSync(let sendClose):
            self.sendCloseAndSyncMessage(sendClose, context: context)
        case .succeedClose(let closeContext):
            closeContext.promise.succeed(Void())
        case .failClose(let closeContext, with: let error, let cleanupContext):
            closeContext.promise.fail(error)
            if let cleanupContext = cleanupContext {
                self.closeConnectionAndCleanup(cleanupContext, context: context)
            }
        case .forwardNotificationToListeners(let notification):
            self.notificationDelegate?.notificationReceived(notification)
        }
    }
    
    // MARK: - Private Methods -
    
    private func connected(context: ChannelHandlerContext) {
        let action = self.state.connected(requireTLS: self.enableSSLCallback != nil)
        
        self.run(action, with: context)
    }
    
    private func establishSSLConnection(context: ChannelHandlerContext) {
        // This method must only be called, if we signalized the StateMachine before that we are
        // able to setup a SSL connection.
        self.enableSSLCallback!(context.channel).whenComplete { result in
            switch result {
            case .success:
                let action = self.state.sslHandlerAdded()
                self.run(action, with: context)
            case .failure(let error):
                let action = self.state.errorHappened(.failedToAddSSLHandler(underlying: error))
                self.run(action, with: context)
            }
        }
    }
    
    private func sendPasswordMessage(
        mode: PasswordAuthencationMode,
        authContext: AuthContext,
        context: ChannelHandlerContext)
    {
        switch mode {
        case .md5(let salt):
            let hash1 = (authContext.password ?? "") + authContext.username
            let pwdhash = Insecure.MD5.hash(data: [UInt8](hash1.utf8)).hexdigest()
            
            var hash2 = [UInt8]()
            hash2.reserveCapacity(pwdhash.count + 4)
            hash2.append(contentsOf: pwdhash.utf8)
            hash2.append(salt.0)
            hash2.append(salt.1)
            hash2.append(salt.2)
            hash2.append(salt.3)
            let hash = "md5" + Insecure.MD5.hash(data: hash2).hexdigest()
            
            context.writeAndFlush(.password(.init(value: hash)), promise: nil)
        case .cleartext:
            context.writeAndFlush(.password(.init(value: authContext.password ?? "")), promise: nil)
        }
    }
    
    private func sendCloseAndSyncMessage(_ sendClose: CloseTarget, context: ChannelHandlerContext) {
        switch sendClose {
        case .preparedStatement(let name):
            context.write(.close(.preparedStatement(name)), promise: nil)
            context.write(.sync, promise: nil)
            context.flush()
        case .portal(let name):
            context.write(.close(.portal(name)), promise: nil)
            context.write(.sync, promise: nil)
            context.flush()
        }
    }
    
    private func sendParseDecribeAndSyncMessage(
        statementName: String,
        query: String,
        context: ChannelHandlerContext)
    {
        precondition(self.rowStream == nil, "Expected to not have an open query at this point")
        let parse = PSQLFrontendMessage.Parse(
            preparedStatementName: statementName,
            query: query,
            parameters: [])
        
        context.write(.parse(parse), promise: nil)
        context.write(.describe(.preparedStatement(statementName)), promise: nil)
        context.write(.sync, promise: nil)
        context.flush()
    }
    
    private func sendBindExecuteAndSyncMessage(
        statementName: String,
        binds: [PSQLEncodable],
        context: ChannelHandlerContext)
    {
        let bind = PSQLFrontendMessage.Bind(
            portalName: "",
            preparedStatementName: statementName,
            parameters: binds)
        
        context.write(.bind(bind), promise: nil)
        context.write(.execute(.init(portalName: "")), promise: nil)
        context.write(.sync, promise: nil)
        context.flush()
    }
    
    private func sendParseDescribeBindExecuteAndSyncMessage(
        query: String, binds: [PSQLEncodable],
        context: ChannelHandlerContext)
    {
        precondition(self.rowStream == nil, "Expected to not have an open query at this point")
        let unnamedStatementName = ""
        let parse = PSQLFrontendMessage.Parse(
            preparedStatementName: unnamedStatementName,
            query: query,
            parameters: binds.map { $0.psqlType })
        let bind = PSQLFrontendMessage.Bind(
            portalName: "",
            preparedStatementName: unnamedStatementName,
            parameters: binds)
        
        context.write(wrapOutboundOut(.parse(parse)), promise: nil)
        context.write(wrapOutboundOut(.describe(.preparedStatement(""))), promise: nil)
        context.write(wrapOutboundOut(.bind(bind)), promise: nil)
        context.write(wrapOutboundOut(.execute(.init(portalName: ""))), promise: nil)
        context.write(wrapOutboundOut(.sync), promise: nil)
        context.flush()
    }
    
    private func succeedQueryWithRowStream(
        _ queryContext: ExtendedQueryContext,
        columns: [PSQLBackendMessage.RowDescription.Column],
        context: ChannelHandlerContext)
    {
        let rows = PSQLRowBatchStream(
            rowDescription: columns,
            queryContext: queryContext,
            eventLoop: context.channel.eventLoop,
            rowSource: .stream(self))
        
        self.rowStream = rows
        queryContext.promise.succeed(rows)
    }
    
    private func succeedQueryWithoutRowStream(
        _ queryContext: ExtendedQueryContext,
        commandTag: String,
        context: ChannelHandlerContext)
    {
        let rows = PSQLRowBatchStream(
            rowDescription: [],
            queryContext: queryContext,
            eventLoop: context.channel.eventLoop,
            rowSource: .noRows(.success(commandTag)))
        queryContext.promise.succeed(rows)
    }
    
    private func closeConnectionAndCleanup(
        _ cleanup: ConnectionStateMachine.ConnectionAction.CleanUpContext,
        context: ChannelHandlerContext)
    {
        self.logger.debug("Cleaning up and closing connection.", metadata: [.error: "\(cleanup.error)"])
        
        // 1. fail all tasks
        cleanup.tasks.forEach { task in
            task.failWithError(cleanup.error)
        }
        
        // 2. fire an error
        context.fireErrorCaught(cleanup.error)
        
        // 3. close the connection or fire channel inactive
        switch cleanup.action {
        case .close:
            context.close(mode: .all, promise: cleanup.closePromise)
        case .fireChannelInactive:
            cleanup.closePromise?.succeed(())
            context.fireChannelInactive()
        }
    }
}

extension PSQLChannelHandler: PSQLRowsDataSource {
    
    func request() {
        let action = self.state.requestQueryRows()
        self.run(action, with: self.handlerContext!)
    }
    
    func cancel() {
        // we ignore this right now :)
    }
    
}

extension ChannelHandlerContext {
    func write(_ psqlMessage: PSQLFrontendMessage, promise: EventLoopPromise<Void>? = nil) {
        self.write(NIOAny(psqlMessage), promise: promise)
    }
    
    func writeAndFlush(_ psqlMessage: PSQLFrontendMessage, promise: EventLoopPromise<Void>? = nil) {
        self.writeAndFlush(NIOAny(psqlMessage), promise: promise)
    }
}

extension PSQLConnection.Configuration.Authentication {
    func toAuthContext() -> AuthContext {
        AuthContext(
            username: self.username,
            password: self.password,
            database: self.database)
    }
}

extension AuthContext {
    func toStartupParameters() -> PSQLFrontendMessage.Startup.Parameters {
        PSQLFrontendMessage.Startup.Parameters(
            user: self.username,
            database: self.database,
            options: nil,
            replication: .false)
    }
}

