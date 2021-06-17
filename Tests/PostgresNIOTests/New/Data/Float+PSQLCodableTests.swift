import XCTest
@testable import PostgresNIO

class Float_PSQLCodableTests: XCTestCase {
    
    func testRoundTripDoubles() {
        let values: [Double] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .forTests())
            XCTAssertEqual(value.psqlType, .float8)
            XCTAssertEqual(buffer.readableBytes, 8)
            let data = PSQLData(bytes: buffer, dataType: .float8, format: .binary)
            
            var result: Double?
            XCTAssertNoThrow(result = try data.decode(as: Double.self, context: .forTests()))
            XCTAssertEqual(value, result)
        }
    }
    
    func testRoundTripFloat() {
        let values: [Float] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .forTests())
            XCTAssertEqual(value.psqlType, .float4)
            XCTAssertEqual(buffer.readableBytes, 4)
            let data = PSQLData(bytes: buffer, dataType: .float4, format: .binary)
            
            var result: Float?
            XCTAssertNoThrow(result = try data.decode(as: Float.self, context: .forTests()))
            XCTAssertEqual(value, result)
        }
    }
    
    func testRoundTripDoubleNaN() {
        let value: Double = .nan
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .float8)
        XCTAssertEqual(buffer.readableBytes, 8)
        let data = PSQLData(bytes: buffer, dataType: .float8, format: .binary)
        
        var result: Double?
        XCTAssertNoThrow(result = try data.decode(as: Double.self, context: .forTests()))
        XCTAssertEqual(result?.isNaN, true)
    }
    
    func testRoundTripDoubleInfinity() {
        let value: Double = .infinity
        
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(value.psqlType, .float8)
        XCTAssertEqual(buffer.readableBytes, 8)
        let data = PSQLData(bytes: buffer, dataType: .float8, format: .binary)
        
        var result: Double?
        XCTAssertNoThrow(result = try data.decode(as: Double.self, context: .forTests()))
        XCTAssertEqual(result?.isInfinite, true)
    }
    
    func testRoundTripFromFloatToDouble() {
        let values: [Float] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .forTests())
            XCTAssertEqual(value.psqlType, .float4)
            XCTAssertEqual(buffer.readableBytes, 4)
            let data = PSQLData(bytes: buffer, dataType: .float4, format: .binary)
            
            var result: Double?
            XCTAssertNoThrow(result = try data.decode(as: Double.self, context: .forTests()))
            XCTAssertEqual(result, Double(value))
        }
    }
    
    func testRoundTripFromDoubleToFloat() {
        let values: [Double] = [1.1, .pi, -5e-12]
        
        for value in values {
            var buffer = ByteBuffer()
            value.encode(into: &buffer, context: .forTests())
            XCTAssertEqual(value.psqlType, .float8)
            XCTAssertEqual(buffer.readableBytes, 8)
            let data = PSQLData(bytes: buffer, dataType: .float8, format: .binary)
            
            var result: Float?
            XCTAssertNoThrow(result = try data.decode(as: Float.self, context: .forTests()))
            XCTAssertEqual(result, Float(value))
        }
    }
    
    func testDecodeFailureInvalidLength() {
        var eightByteBuffer = ByteBuffer()
        eightByteBuffer.writeInteger(Int64(0))
        var fourByteBuffer = ByteBuffer()
        fourByteBuffer.writeInteger(Int32(0))
        let toLongData = PSQLData(bytes: eightByteBuffer, dataType: .float4, format: .binary)
        let toShortData = PSQLData(bytes: fourByteBuffer, dataType: .float8, format: .binary)
        
        XCTAssertThrowsError(try toLongData.decode(as: Double.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
        
        XCTAssertThrowsError(try toLongData.decode(as: Float.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
        
        XCTAssertThrowsError(try toShortData.decode(as: Double.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
        
        XCTAssertThrowsError(try toShortData.decode(as: Float.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeFailureInvalidType() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int64(0))
        let data = PSQLData(bytes: buffer, dataType: .int8, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: Double.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
        
        XCTAssertThrowsError(try data.decode(as: Float.self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
}
