import XCTest
@testable import PostgresNIO

class PrepareStatementStateMachineTests: XCTestCase {
    
    func testCreatePreparedStatementReturningRowDescription() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let promise = EmbeddedEventLoop().makePromise(of: PSQLBackendMessage.RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        
        let name = "haha"
        let query = #"SELECT id FROM users WHERE id = $1 "#
        let prepareStatementContext = PrepareStatementContext(
            name: name, query: query, logger: .psqlTest, promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .preparedStatement(prepareStatementContext)),
                       .sendParseDescribeSync(name: name, query: query))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        let columns: [PSQLBackendMessage.RowDescription.Column] = [
            .init(name: "id", tableOID: 0, columnAttributeNumber: 0, dataType: .int8, dataTypeSize: 8, dataTypeModifier: -1, format: .binary)
        ]
        
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: columns)),
                       .succeedPreparedStatementCreation(prepareStatementContext, with: .init(columns: columns)))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testCreatePreparedStatementReturningNoData() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let promise = EmbeddedEventLoop().makePromise(of: PSQLBackendMessage.RowDescription?.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        
        let name = "haha"
        let query = #"DELETE FROM users WHERE id = $1 "#
        let prepareStatementContext = PrepareStatementContext(
            name: name, query: query, logger: .psqlTest, promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .preparedStatement(prepareStatementContext)),
                       .sendParseDescribeSync(name: name, query: query))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        XCTAssertEqual(state.noDataReceived(),
                       .succeedPreparedStatementCreation(prepareStatementContext, with: nil))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testErrorReceivedAfter() {
        let connectionContext = ConnectionStateMachine.createConnectionContext()
        var state = ConnectionStateMachine(.prepareStatement(.init(.noDataMessageReceived), connectionContext))
        
        XCTAssertEqual(state.authenticationMessageReceived(.ok),
                       .closeConnectionAndCleanup(.init(action: .close, tasks: [], error: .unexpectedBackendMessage(.authentication(.ok)), closePromise: nil)))
    }

}
