import XCTest
@testable import PostgresNIO

#if false
class ExtendedQueryStateMachineTests: XCTestCase {
    
    func testExtendedQueryWithoutDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRows.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "DELETE FROM table WHERE id=$0"
        let queryContext = ExtendedQueryContext(query: query, bind: [1], logger: logger, jsonDecoder: JSONDecoder(), promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext)), .sendParseDescribeBindExecuteSync(query: query, binds: [1]))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        XCTAssertEqual(state.noDataReceived(), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .wait)
        XCTAssertEqual(state.commandCompletedReceived("DELETE 1"), .succeedQueryNoRowsComming(queryContext, commandTag: "DELETE 1"))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testExtendedQueryWithDataRowsHappyPath() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let queryPromise = EmbeddedEventLoop().makePromise(of: PSQLRows.self)
        queryPromise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "SELECT version()"
        let queryContext = ExtendedQueryContext(query: query, bind: [], logger: logger, jsonDecoder: JSONDecoder(), promise: queryPromise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext)), .sendParseDescribeBindExecuteSync(query: query, binds: []))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        let columns: [PSQLBackendMessage.RowDescription.Column] = [
            .init(name: "version", tableOID: 0, columnAttributeNumber: 0, dataType: .text, dataTypeSize: -1, dataTypeModifier: -1, formatCode: .text)
        ]
        
        XCTAssertEqual(state.rowDescriptionReceived(.init(columns: columns)), .wait)
        XCTAssertEqual(state.bindCompleteReceived(), .succeedQuery(queryContext, columns: columns))
        let rowContent = ByteBuffer(string: "test")
        XCTAssertEqual(state.dataRowReceived(.init(columns: [rowContent])), .wait)
        XCTAssertEqual(state.readEventCaught(), .wait)
        
        let rowPromise = EmbeddedEventLoop().makePromise(of: StateMachineStreamNextResult.self)
        rowPromise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        XCTAssertEqual(state.consumeNextQueryRow(promise: rowPromise), .forwardRows(.init(arrayLiteral: [.init(bytes: rowContent, dataType: .text)]), to: rowPromise))
        
        XCTAssertEqual(state.commandCompletedReceived("SELECT 1"), .forwardStreamCompletedToCurrentQuery(CircularBuffer(), commandTag: "SELECT 1", read: true))
        XCTAssertEqual(state.readyForQueryReceived(.idle), .fireEventReadyForQuery)
    }
    
    func testReceiveTotallyUnexpectedMessageInQuery() {
        var state = ConnectionStateMachine.readyForQuery()
        
        let logger = Logger.psqlTest
        let promise = EmbeddedEventLoop().makePromise(of: PSQLRows.self)
        promise.fail(PSQLError.uncleanShutdown) // we don't care about the error at all.
        let query = "DELETE FROM table WHERE id=$0"
        let queryContext = ExtendedQueryContext(query: query, bind: [1], logger: logger, jsonDecoder: JSONDecoder(), promise: promise)
        
        XCTAssertEqual(state.enqueue(task: .extendedQuery(queryContext)), .sendParseDescribeBindExecuteSync(query: query, binds: [1]))
        XCTAssertEqual(state.parseCompleteReceived(), .wait)
        XCTAssertEqual(state.parameterDescriptionReceived(.init(dataTypes: [.int8])), .wait)
        
        let psqlError = PSQLError.unexpectedBackendMessage(.authentication(.ok))
        XCTAssertEqual(state.authenticationMessageReceived(.ok),
                       .failQuery(queryContext, with: psqlError, cleanupContext: .init(action: .close, tasks: [], error: psqlError, closePromise: nil)))
    }

}
#endif
