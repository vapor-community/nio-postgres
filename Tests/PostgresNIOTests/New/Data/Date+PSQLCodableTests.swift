import XCTest
@testable import PostgresNIO

class Date_PSQLCodableTests: XCTestCase {
    
    func testNowRoundTrip() {
        let value = Date()
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .timestamptz)
        XCTAssertEqual(buffer.readableBytes, 8)
        let data = PSQLData(bytes: buffer, dataType: .timestamptz, format: .binary)
        
        var result: Date?
        XCTAssertNoThrow(result = try data.decode(as: Date.self, context: .forTests()))
        XCTAssertEqual(value, result)
    }
    
    func testDecodeRandomDate() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        let data = PSQLData(bytes: buffer, dataType: .timestamptz, format: .binary)
        
        var result: Date?
        XCTAssertNoThrow(result = try data.decode(as: Date.self, context: .forTests()))
        XCTAssertNotNil(result)
    }
    
    func testDecodeFailureInvalidLength() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        buffer.writeInteger(Int64.random(in: Int64.min...Int64.max))
        let data = PSQLData(bytes: buffer, dataType: .timestamptz, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Date.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeDate() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)
        let firstDateData = PSQLData(bytes: firstDateBuffer, dataType: .date, format: .binary)
        
        var firstDate: Date?
        XCTAssertNoThrow(firstDate = try firstDateData.decode(as: Date.self, context: .forTests()))
        XCTAssertNotNil(firstDate)
        
        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)
        let lastDateData = PSQLData(bytes: lastDateBuffer, dataType: .date, format: .binary)
        
        var lastDate: Date?
        XCTAssertNoThrow(lastDate = try lastDateData.decode(as: Date.self, context: .forTests()))
        XCTAssertNotNil(lastDate)
    }
    
    func testDecodeDateFromTimestamp() {
        var firstDateBuffer = ByteBuffer()
        firstDateBuffer.writeInteger(Int32.min)
        let firstDateData = PSQLData(bytes: firstDateBuffer, dataType: .date, format: .binary)
        
        var firstDate: Date?
        XCTAssertNoThrow(firstDate = try firstDateData.decode(as: Date.self, context: .forTests()))
        XCTAssertNotNil(firstDate)
        
        var lastDateBuffer = ByteBuffer()
        lastDateBuffer.writeInteger(Int32.max)
        let lastDateData = PSQLData(bytes: lastDateBuffer, dataType: .date, format: .binary)
        
        var lastDate: Date?
        XCTAssertNoThrow(lastDate = try lastDateData.decode(as: Date.self, context: .forTests()))
        XCTAssertNotNil(lastDate)
    }
    
    func testDecodeDateFailsWithToMuchData() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))
        let data = PSQLData(bytes: buffer, dataType: .date, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Date.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeDateFailsWithWrongDataType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))
        let data = PSQLData(bytes: buffer, dataType: .int8, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Date.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
}
