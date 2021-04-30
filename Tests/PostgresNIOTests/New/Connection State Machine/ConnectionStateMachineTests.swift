import XCTest
@testable import PostgresNIO
@testable import NIO

class ConnectionStateMachineTests: XCTestCase {
    
    func testStartup() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine()
        XCTAssertEqual(state.connected(requireTLS: false), .provideAuthenticationContext)
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.plaintext), .sendPasswordMessage(.cleartext, authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.ok), .wait)
    }
    
    func testSSLStartupSuccess() {
        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        var state = ConnectionStateMachine()
        XCTAssertEqual(state.connected(requireTLS: true), .sendSSLRequest)
        XCTAssertEqual(state.sslSupportedReceived(), .establishSSLConnection)
        XCTAssertEqual(state.sslHandlerAdded(), .wait)
        XCTAssertEqual(state.sslEstablished(), .provideAuthenticationContext)
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        let salt: (UInt8, UInt8, UInt8, UInt8) = (0,1,2,3)
        XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
    }
    
    func testSSLStartupSSLUnsupported() {
        var state = ConnectionStateMachine()
        
        XCTAssertEqual(state.connected(requireTLS: true), .sendSSLRequest)
        XCTAssertEqual(state.sslUnsupportedReceived(),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .sslUnsupported, closePromise: nil)))
    }
        
    func testParameterStatusReceivedAndBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.backendKeyDataReceived(.init(processID: 2730, secretKey: 882037977)), .wait)
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testBackendKeyAndParameterStatusReceivedAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        XCTAssertEqual(state.backendKeyDataReceived(.init(processID: 2730, secretKey: 882037977)), .wait)
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testReadyForQueryReceivedWithoutBackendKeyAfterAuthenticated() {
        var state = ConnectionStateMachine(.authenticated(nil, [:]))
        
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "DateStyle", value: "ISO, MDY")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "application_name", value: "")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "integer_datetimes", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "client_encoding", value: "UTF8")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "TimeZone", value: "Etc/UTC")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "is_superuser", value: "on")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "session_authorization", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "IntervalStyle", value: "postgres")), .wait)
        XCTAssertEqual(state.parameterStatusReceived(.init(parameter: "standard_conforming_strings", value: "on")), .wait)
        
        XCTAssertEqual(state.readyForQueryReceived(.idle),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unexpectedBackendMessage(.readyForQuery(.idle)), closePromise: nil)))
    }
    
    func testErrorIsIgnoredWhenClosingConnection() {
        // test ignore unclean shutdown when closing connection
        var stateIgnoreChannelError = ConnectionStateMachine(.closing)
        
        XCTAssertEqual(stateIgnoreChannelError.errorHappened(.channel(underlying: NIOSSLError.uncleanShutdown)), .wait)
        XCTAssertEqual(stateIgnoreChannelError.closed(), .fireChannelInactive)
        
        // test ignore any other error when closing connection
        
        var stateIgnoreErrorMessage = ConnectionStateMachine(.closing)
        XCTAssertEqual(stateIgnoreErrorMessage.errorReceived(.init(fields: [:])), .wait)
        XCTAssertEqual(stateIgnoreErrorMessage.closed(), .fireChannelInactive)
    }
    
    func testFailQueuedQueriesOnAuthenticationFailure() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }

        let authContext = AuthContext(username: "test", password: "abc123", database: "test")
        let salt: (UInt8, UInt8, UInt8, UInt8) = (0, 1, 2, 3)

        let jsonDecoder = JSONDecoder()
        let queryPromise = eventLoopGroup.next().makePromise(of: PSQLRowBatchStream.self)

        var state = ConnectionStateMachine()
        let extendedQueryContext = ExtendedQueryContext(
            query: "Select version()",
            bind: [],
            logger: .psqlTest,
            jsonDecoder: jsonDecoder,
            promise: queryPromise)

        XCTAssertEqual(state.enqueue(task: .extendedQuery(extendedQueryContext)), .wait)
        XCTAssertEqual(state.connected(requireTLS: false), .provideAuthenticationContext)
        XCTAssertEqual(state.provideAuthenticationContext(authContext), .sendStartupMessage(authContext))
        XCTAssertEqual(state.authenticationMessageReceived(.md5(salt: salt)), .sendPasswordMessage(.md5(salt: salt), authContext))
        let fields: [PSQLBackendMessage.Field: String] = [
            .message: "password authentication failed for user \"postgres\"",
            .severity: "FATAL",
            .sqlState: "28P01",
            .localizedSeverity: "FATAL",
            .routine: "auth_failed",
            .line: "334",
            .file: "auth.c"
        ]
        XCTAssertEqual(state.errorReceived(.init(fields: fields)),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [.extendedQuery(extendedQueryContext)], error: .server(.init(fields: fields)), closePromise: nil)))
        
        XCTAssertNil(extendedQueryContext.promise.futureResult._value)
        
        // make sure we don't crash
        extendedQueryContext.promise.fail(PSQLError.server(.init(fields: fields)))
    }
}
