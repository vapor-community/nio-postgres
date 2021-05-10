import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class ParameterDescriptionTests: XCTestCase {
    
    func testDecode() {
        let expected: [PSQLBackendMessage] = [
            .parameterDescription(.init(dataTypes: [.bool, .varchar, .uuid, .json, .jsonbArray])),
        ]
        
        var buffer = ByteBuffer()
        expected.forEach { message in
            guard case .parameterDescription(let description) = message else {
                return XCTFail("Expected only to get parameter descriptions here!")
            }
            
            buffer.writeBackendMessage(id: .parameterDescription) { buffer in
                buffer.writeInteger(Int16(description.dataTypes.count))
                
                description.dataTypes.forEach { dataType in
                    buffer.writeInteger(dataType.rawValue)
                }
            }
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) }))
    }
    
    func testDecodeWithNegativeCount() {
        let dataTypes: [PSQLDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            buffer.writeInteger(Int16(-4))
            
            dataTypes.forEach { dataType in
                buffer.writeInteger(dataType.rawValue)
            }
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
    
    func testDecodeColumnCountDoesntMatchMessageLength() {
        let dataTypes: [PSQLDataType] = [.bool, .varchar, .uuid, .json, .jsonbArray]
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterDescription) { buffer in
            // means three columns comming, but 5 are in the buffer actually.
            buffer.writeInteger(Int16(3))
            
            dataTypes.forEach { dataType in
                buffer.writeInteger(dataType.rawValue)
            }
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
}
