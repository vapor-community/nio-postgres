import XCTest
@testable import PostgresNIO

class Optional_PSQLCodableTests: XCTestCase {
    
    func testRoundTripSomeString() {
        let value: String? = "Hello World"
        
        var buffer = ByteBuffer()
        value?.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .text)
        let data = PSQLData(bytes: buffer, dataType: .text, format: .binary)
        
        var result: String?
        XCTAssertNoThrow(result = try data.decode(as: String?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneString() {
        let value: Optional<String> = .none
        
        var buffer = ByteBuffer()
        value?.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(buffer.readableBytes, 0)
        XCTAssertEqual(value.psqlType, .null)
        
        let data = PSQLData(bytes: nil, dataType: .text, format: .binary)
        
        var result: String?
        XCTAssertNoThrow(result = try data.decode(as: String?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripSomeUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = UUID()
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .uuid)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 20)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), 16)
        let data = PSQLData(bytes: buffer, dataType: .uuid, format: .binary)
        
        var result: UUID?
        XCTAssertNoThrow(result = try data.decode(as: UUID?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
    
    func testRoundTripNoneUUIDAsPSQLEncodable() {
        let value: Optional<UUID> = .none
        let encodable: PSQLEncodable = value
        
        var buffer = ByteBuffer()
        XCTAssertEqual(encodable.psqlType, .null)
        XCTAssertNoThrow(try encodable.encodeRaw(into: &buffer, context: .forTests()))
        XCTAssertEqual(buffer.readableBytes, 4)
        XCTAssertEqual(buffer.readInteger(as: Int32.self), -1)
        
        let data = PSQLData(bytes: nil, dataType: .uuid, format: .binary)
        
        var result: UUID?
        XCTAssertNoThrow(result = try data.decode(as: UUID?.self, context: .forTests()))
        XCTAssertEqual(result, value)
    }
}
