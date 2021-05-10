import XCTest
@testable import PostgresNIO

class Array_PSQLCodableTests: XCTestCase {
    
    func testArrayTypes() {

        XCTAssertEqual(Bool.psqlArrayType, .boolArray)
        XCTAssertEqual(Bool.psqlArrayElementType, .bool)
        XCTAssertEqual([Bool]().psqlType, .boolArray)

        XCTAssertEqual(ByteBuffer.psqlArrayType, .byteaArray)
        XCTAssertEqual(ByteBuffer.psqlArrayElementType, .bytea)
        XCTAssertEqual([ByteBuffer]().psqlType, .byteaArray)

        XCTAssertEqual(UInt8.psqlArrayType, .charArray)
        XCTAssertEqual(UInt8.psqlArrayElementType, .char)
        XCTAssertEqual([UInt8]().psqlType, .charArray)

        XCTAssertEqual(Int16.psqlArrayType, .int2Array)
        XCTAssertEqual(Int16.psqlArrayElementType, .int2)
        XCTAssertEqual([Int16]().psqlType, .int2Array)

        XCTAssertEqual(Int32.psqlArrayType, .int4Array)
        XCTAssertEqual(Int32.psqlArrayElementType, .int4)
        XCTAssertEqual([Int32]().psqlType, .int4Array)

        XCTAssertEqual(Int64.psqlArrayType, .int8Array)
        XCTAssertEqual(Int64.psqlArrayElementType, .int8)
        XCTAssertEqual([Int64]().psqlType, .int8Array)
        
        #if (arch(i386) || arch(arm))
        XCTAssertEqual(Int.psqlArrayType, .int4Array)
        XCTAssertEqual(Int.psqlArrayElementType, .int4)
        XCTAssertEqual([Int]().psqlType, .int4Array)
        #else
        XCTAssertEqual(Int.psqlArrayType, .int8Array)
        XCTAssertEqual(Int.psqlArrayElementType, .int8)
        XCTAssertEqual([Int]().psqlType, .int8Array)
        #endif

        XCTAssertEqual(Float.psqlArrayType, .float4Array)
        XCTAssertEqual(Float.psqlArrayElementType, .float4)
        XCTAssertEqual([Float]().psqlType, .float4Array)

        XCTAssertEqual(Double.psqlArrayType, .float8Array)
        XCTAssertEqual(Double.psqlArrayElementType, .float8)
        XCTAssertEqual([Double]().psqlType, .float8Array)

        XCTAssertEqual(String.psqlArrayType, .textArray)
        XCTAssertEqual(String.psqlArrayElementType, .text)
        XCTAssertEqual([String]().psqlType, .textArray)

        XCTAssertEqual(UUID.psqlArrayType, .uuidArray)
        XCTAssertEqual(UUID.psqlArrayElementType, .uuid)
        XCTAssertEqual([UUID]().psqlType, .uuidArray)
    }
    
    func testStringArrayRoundTrip() {
        let values = ["foo", "bar", "hello", "world"]
        
        var buffer = ByteBuffer()
        XCTAssertNoThrow(try values.encode(into: &buffer, context: .forTests()))
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        var result: [String]?
        XCTAssertNoThrow(result = try data.decode(as: [String].self, context: .forTests()))
        XCTAssertEqual(values, result)
    }
    
    func testEmptyStringArrayRoundTrip() {
        let values: [String] = []
        
        var buffer = ByteBuffer()
        XCTAssertNoThrow(try values.encode(into: &buffer, context: .forTests()))
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        var result: [String]?
        XCTAssertNoThrow(result = try data.decode(as: [String].self, context: .forTests()))
        XCTAssertEqual(values, result)
    }
    
    func testDecodeFailureIsNotEmptyOutOfScope() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(2)) // invalid value
        buffer.writeInteger(Int32(0))
        buffer.writeInteger(String.psqlArrayElementType.rawValue)
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeFailureSecondValueIsUnexpected() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(0)) // is empty
        buffer.writeInteger(Int32(1)) // invalid value, must always be 0
        buffer.writeInteger(String.psqlArrayElementType.rawValue)
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeFailureTriesDecodeInt8() {
        let value: Int64 = 1 << 32
        var buffer = ByteBuffer()
        value.encode(into: &buffer, context: .forTests())
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeFailureInvalidNumberOfArrayElements() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // invalid value
        buffer.writeInteger(Int32(0))
        buffer.writeInteger(String.psqlArrayElementType.rawValue)
        buffer.writeInteger(Int32(-123)) // expected element count
        buffer.writeInteger(Int32(1)) // dimensions... must be one
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeFailureInvalidNumberOfDimensions() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // invalid value
        buffer.writeInteger(Int32(0))
        buffer.writeInteger(String.psqlArrayElementType.rawValue)
        buffer.writeInteger(Int32(1)) // expected element count
        buffer.writeInteger(Int32(2)) // dimensions... must be one
        let data = PSQLData(bytes: buffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
    
    func testDecodeUnexpectedEnd() {
        var unexpectedEndInElementLengthBuffer = ByteBuffer()
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(1)) // invalid value
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(0))
        unexpectedEndInElementLengthBuffer.writeInteger(String.psqlArrayElementType.rawValue)
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(1)) // expected element count
        unexpectedEndInElementLengthBuffer.writeInteger(Int32(1)) // dimensions
        unexpectedEndInElementLengthBuffer.writeInteger(Int16(1)) // length of element, must be Int32
        let data = PSQLData(bytes: unexpectedEndInElementLengthBuffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
        
        var unexpectedEndInElementBuffer = ByteBuffer()
        unexpectedEndInElementBuffer.writeInteger(Int32(1)) // invalid value
        unexpectedEndInElementBuffer.writeInteger(Int32(0))
        unexpectedEndInElementBuffer.writeInteger(String.psqlArrayElementType.rawValue)
        unexpectedEndInElementBuffer.writeInteger(Int32(1)) // expected element count
        unexpectedEndInElementBuffer.writeInteger(Int32(1)) // dimensions
        unexpectedEndInElementBuffer.writeInteger(Int32(12)) // length of element, must be Int32
        unexpectedEndInElementBuffer.writeString("Hello World") // only 11 bytes, 12 needed!
        let unexpectedEndInElementData = PSQLData(bytes: unexpectedEndInElementBuffer, dataType: .textArray, format: .binary)
        
        XCTAssertThrowsError(try unexpectedEndInElementData.decode(as: [String].self, context: .forTests())) { error in
            XCTAssert(error is PSQLCastingError)
        }
    }
}
