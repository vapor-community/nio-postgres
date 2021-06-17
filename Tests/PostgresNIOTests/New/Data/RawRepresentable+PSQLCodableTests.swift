import XCTest
@testable import PostgresNIO

class RawRepresentable_PSQLCodableTests: XCTestCase {
    
    enum MyRawRepresentable: Int16, PSQLCodable {
        case testing = 1
        case staging = 2
        case production = 3
    }
    
    func testRoundTrip() {
        let values: [MyRawRepresentable] = [.testing, .staging, .production]
        
        for value in values {
            var buffer = ByteBuffer()
            XCTAssertNoThrow(try value.encode(into: &buffer, context: .forTests()))
            XCTAssertEqual(value.psqlType, Int16.psqlArrayElementType)
            XCTAssertEqual(buffer.readableBytes, 2)
            let data = PSQLData(bytes: buffer, dataType: Int16.psqlArrayElementType, format: .binary)
            
            var result: MyRawRepresentable?
            XCTAssertNoThrow(result = try data.decode(as: MyRawRepresentable.self, context: .forTests()))
            XCTAssertEqual(value, result)
        }
    }
    
    func testDecodeInvalidRawTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int16(4)) // out of bounds
        let data = PSQLData(bytes: buffer, dataType: Int16.psqlArrayElementType, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: MyRawRepresentable.self, context: .forTests())) { error in
            XCTAssertEqual((error as? PSQLCastingError)?.line, #line - 1)
            XCTAssertEqual((error as? PSQLCastingError)?.file, #file)
            XCTAssert((error as? PSQLCastingError)?.targetType == MyRawRepresentable.self)
        }
    }
    
    func testDecodeInvalidUnderlyingTypeValue() {
        var buffer = ByteBuffer()
        buffer.writeInteger(Int32(1)) // out of bounds
        let data = PSQLData(bytes: buffer, dataType: Int32.psqlArrayElementType, format: .binary)
        
        XCTAssertThrowsError(try data.decode(as: MyRawRepresentable.self, context: .forTests())) { error in
            XCTAssertEqual((error as? PSQLCastingError)?.line, #line - 1)
            XCTAssertEqual((error as? PSQLCastingError)?.file, #file)
            XCTAssert((error as? PSQLCastingError)?.targetType == MyRawRepresentable.self)
        }
    }
    
}
