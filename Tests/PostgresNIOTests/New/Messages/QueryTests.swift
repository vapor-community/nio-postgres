import XCTest
@testable import PostgresNIO

class QueryTests: XCTestCase {
    
    func testEncodeQuery() {
        let encoder = PSQLFrontendMessage.Encoder.forTests
        var byteBuffer = ByteBuffer()
        let query = "SELECT version()"
        let message = PSQLFrontendMessage.query(.init(value: query))
        XCTAssertNoThrow(try encoder.encode(data: message, out: &byteBuffer))
        
        let expectedLength = 1 + 4 + query.utf8.count + 1
        
        XCTAssertEqual(byteBuffer.readableBytes, expectedLength)
        XCTAssertEqual(byteBuffer.readInteger(as: UInt8.self), PSQLFrontendMessage.ID.query.byte)
        XCTAssertEqual(byteBuffer.readInteger(as: Int32.self), Int32(expectedLength - 1)) // length
        XCTAssertEqual(byteBuffer.readNullTerminatedString(), query)
    }
}
