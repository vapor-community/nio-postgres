import XCTest
@testable import PostgresNIO

class Bool_PSQLCodableTests: XCTestCase {
    
    // MARK: - Binary
    
    func testBinaryTrueRoundTrip() {
        let value = true
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .bool)
        XCTAssertEqual(value.psqlFormat, .binary)
        XCTAssertEqual(buffer.readableBytes, 1)
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 1)
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .binary)
        
        var result: Bool?
        XCTAssertNoThrow(result = try data.decode(as: Bool.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testBinaryFalseRoundTrip() {
        let value = false
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .bool)
        XCTAssertEqual(value.psqlFormat, .binary)
        XCTAssertEqual(buffer.readableBytes, 1)
        XCTAssertEqual(buffer.getInteger(at: buffer.readerIndex, as: UInt8.self), 0)
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .binary)
        
        var result: Bool?
        XCTAssertNoThrow(result = try data.decode(as: Bool.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testBinaryDecodeBoolInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(1))
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Bool.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testBinaryDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Bool.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }

    // MARK: - Text
    
    func testTextTrueDecode() {
        let value = true
        
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "t"))
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .text)
        
        var result: Bool?
        XCTAssertNoThrow(result = try data.decode(as: Bool.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testTextFalseDecode() {
        let value = false
        
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "f"))
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .text)
        
        var result: Bool?
        XCTAssertNoThrow(result = try data.decode(as: Bool.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testTextDecodeBoolInvalidValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(13))
        let data = PSQLData(bytes: buffer, dataType: .bool, format: .text)
        
        XCTAssertThrowsError(try data.decode(as: Bool.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
}
