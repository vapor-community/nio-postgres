import XCTest
@testable import PostgresNIO

class Bytes_PSQLCodableTests: XCTestCase {
    
    func testDataRoundTrip() {
        let data = Data((0...UInt8.max))
        
        var buffer = ByteBuffer()
        data.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(data.psqlType, .bytea)
        let psqlData = PSQLData(bytes: buffer, dataType: .bytea, format: .binary)
        
        var result: Data?
        XCTAssertNoThrow(result = try psqlData.decode(as: Data.self, context: .forTests()))
        XCTAssertEqual(data, result)
    }
    
    func testByteBufferRoundTrip() {
        let bytes = ByteBuffer(bytes: (0...UInt8.max))
        
        var buffer = ByteBuffer()
        bytes.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(bytes.psqlType, .bytea)
        let psqlData = PSQLData(bytes: buffer, dataType: .bytea, format: .binary)
        
        var result: ByteBuffer?
        XCTAssertNoThrow(result = try psqlData.decode(as: ByteBuffer.self, context: .forTests()))
        XCTAssertEqual(bytes, result)
    }
    
    func testEncodeSequenceWhereElementUInt8() {
        struct ByteSequence: Sequence, PSQLEncodable {
            typealias Element = UInt8
            typealias Iterator = Array<UInt8>.Iterator
            
            let bytes: [UInt8]
            
            init() {
                self.bytes = [UInt8]((0...UInt8.max))
            }
            
            func makeIterator() -> Array<UInt8>.Iterator {
                self.bytes.makeIterator()
            }
        }
        
        let sequence = ByteSequence()
        var buffer = ByteBuffer()
        sequence.encode(into: &buffer, context: .forTests())
        XCTAssertEqual(sequence.psqlType, .bytea)
        XCTAssertEqual(buffer.readableBytes, 256)
    }
}
