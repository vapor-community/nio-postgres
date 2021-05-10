import NIO
import XCTest
@testable import PostgresNIO

class PSQLDataTests: XCTestCase {
    func testStringDecoding() {
        let emptyBuffer: ByteBuffer? = nil
        
        let data = PSQLData(bytes: emptyBuffer, dataType: .text, format: .binary)
        
        var emptyResult: String?
        XCTAssertNoThrow(emptyResult = try data.decodeIfPresent(as: String.self, context: .forTests()))
        XCTAssertNil(emptyResult)
        
        XCTAssertNoThrow(emptyResult = try data.decode(as: String?.self, context: .forTests()))
        XCTAssertNil(emptyResult)
    }
}
