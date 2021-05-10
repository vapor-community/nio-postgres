import XCTest
import NIO
@testable import PostgresNIO

class PSQLChannelHandlerTests: XCTestCase {
    
    // MARK: Startup
    
    func testHandlerAddedWithoutSSL() {
        let config = self.testConnectionConfiguration()
        let handler = PSQLChannelHandler(authentification: config.authentication)
        let embedded = EmbeddedChannel(handler: handler)
        defer { XCTAssertNoThrow(try embedded.finish()) }
        
        var maybeMessage: PSQLFrontendMessage?
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertNoThrow(maybeMessage = try embedded.readOutbound(as: PSQLFrontendMessage.self))
        guard case .startup(let startup) = maybeMessage else {
            return XCTFail("Unexpected message")
        }
        
        XCTAssertEqual(startup.parameters.user, config.authentication?.username)
        XCTAssertEqual(startup.parameters.database, config.authentication?.database)
        XCTAssertEqual(startup.parameters.options, nil)
        XCTAssertEqual(startup.parameters.replication, .false)
        
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.authentication(.ok))))
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.backendKeyData(.init(processID: 1234, secretKey: 5678)))))
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.readyForQuery(.idle))))
    }
    
    func testEstablishSSLCallbackIsCalledIfSSLIsSupported() {
        var config = self.testConnectionConfiguration()
        config.tlsConfiguration = .forClient(certificateVerification: .none)
        var addSSLCallbackIsHit = false
        let handler = PSQLChannelHandler(authentification: config.authentication) { channel in
            addSSLCallbackIsHit = true
            return channel.eventLoop.makeSucceededFuture(())
        }
        let embedded = EmbeddedChannel(handler: handler)
        
        var maybeMessage: PSQLFrontendMessage?
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertNoThrow(maybeMessage = try embedded.readOutbound(as: PSQLFrontendMessage.self))
        guard case .sslRequest(let request) = maybeMessage else {
            return XCTFail("Unexpected message")
        }
        
        XCTAssertEqual(request.code, 80877103)
        
        // first we need to add an encoder, because NIOSSLHandler can only
        // operate on ByteBuffer
        let future = embedded.pipeline.addHandlers(MessageToByteHandler(PSQLFrontendMessage.Encoder.forTests), position: .first)
        XCTAssertNoThrow(try future.wait())
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.sslSupported)))
        
        // a NIOSSLHandler has been added, after it SSL had been negotiated
        XCTAssertTrue(addSSLCallbackIsHit)
    }
    
    func testSSLUnsupportedClosesConnection() {
        var config = self.testConnectionConfiguration()
        config.tlsConfiguration = .forClient()
        
        let handler = PSQLChannelHandler(authentification: config.authentication) { channel in
            XCTFail("This callback should never be exectuded")
            return channel.eventLoop.makeFailedFuture(PSQLError.sslUnsupported)
        }
        let embedded = EmbeddedChannel(handler: handler)
        let eventHandler = TestEventHandler()
        XCTAssertNoThrow(try embedded.pipeline.addHandler(eventHandler, position: .last).wait())
        
        XCTAssertNoThrow(embedded.connect(to: try .init(ipAddress: "0.0.0.0", port: 5432), promise: nil))
        XCTAssertTrue(embedded.isActive)
        
        // read the ssl request message
        XCTAssertEqual(try embedded.readOutbound(as: PSQLFrontendMessage.self), .sslRequest(.init()))
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.sslUnsupported)))
        
        // the event handler should have seen an error
        XCTAssertEqual(eventHandler.errors.count, 1)
        
        // the connections should be closed
        XCTAssertFalse(embedded.isActive)
    }
    
    // MARK: Run Actions
    
    func testRunAuthenticateMD5Password() {
        let config = self.testConnectionConfiguration()
        let authContext = AuthContext(
            username: config.authentication?.username ?? "something wrong",
            password: config.authentication?.password,
            database: config.authentication?.database
        )
        let state = ConnectionStateMachine(.waitingToStartAuthentication)
        let handler = PSQLChannelHandler(authentification: config.authentication, state: state)
        let embedded = EmbeddedChannel(handler: handler)
        
        embedded.triggerUserOutboundEvent(PSQLOutgoingEvent.authenticate(authContext), promise: nil)
        XCTAssertEqual(try embedded.readOutbound(as: PSQLFrontendMessage.self), .startup(.versionThree(parameters: authContext.toStartupParameters())))
        
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.authentication(.md5(salt: (0,1,2,3))))))
        
        var message: PSQLFrontendMessage?
        XCTAssertNoThrow(message = try embedded.readOutbound(as: PSQLFrontendMessage.self))
        
        XCTAssertEqual(message, .password(.init(value: "md522d085ed8dc3377968dc1c1a40519a2a")))
    }
    
    func testRunAuthenticateCleartext() {
        let password = "postgres"
        var config = self.testConnectionConfiguration()
        config.authentication?.password = password
        
        let authContext = AuthContext(
            username: config.authentication?.username ?? "something wrong",
            password: config.authentication?.password,
            database: config.authentication?.database
        )
        let state = ConnectionStateMachine(.waitingToStartAuthentication)
        let handler = PSQLChannelHandler(authentification: config.authentication, state: state)
        let embedded = EmbeddedChannel(handler: handler)
        
        embedded.triggerUserOutboundEvent(PSQLOutgoingEvent.authenticate(authContext), promise: nil)
        XCTAssertEqual(try embedded.readOutbound(as: PSQLFrontendMessage.self), .startup(.versionThree(parameters: authContext.toStartupParameters())))
        
        XCTAssertNoThrow(try embedded.writeInbound(PSQLOptimizedBackendMessage.pure(.authentication(.plaintext))))
        
        var message: PSQLFrontendMessage?
        XCTAssertNoThrow(message = try embedded.readOutbound(as: PSQLFrontendMessage.self))
        
        XCTAssertEqual(message, .password(.init(value: password)))
    }
    
    // MARK: Helpers
    
    func testConnectionConfiguration(
        host: String = "127.0.0.1",
        port: Int = 5432,
        username: String = "test",
        database: String = "postgres",
        password: String = "password",
        tlsConfiguration: TLSConfiguration? = nil
    ) -> PSQLConnection.Configuration {
        PSQLConnection.Configuration(
            host: host,
            port: port,
            username: username,
            database: database,
            password: password,
            tlsConfiguration: tlsConfiguration,
            coders: .foundation)
    }
}

class TestEventHandler: ChannelInboundHandler {
    typealias InboundIn = Never
    
    var errors = [PSQLError]()
    var events = [PSQLEvent]()
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        guard let psqlError = error as? PSQLError else {
            return XCTFail("Unexpected error type received: \(error)")
        }
        self.errors.append(psqlError)
    }
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        guard let psqlEvent = event as? PSQLEvent else {
            return XCTFail("Unexpected event type received: \(event)")
        }
        self.events.append(psqlEvent)
    }
}
