import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class ParameterStatusTests: XCTestCase {
    
    func testDecode() {
        var buffer = ByteBuffer()
        
        let expected: [PSQLBackendMessage] = [
            .parameterStatus(.init(parameter: "DateStyle", value: "ISO, MDY")),
            .parameterStatus(.init(parameter: "application_name", value: "")),
            .parameterStatus(.init(parameter: "server_encoding", value: "UTF8")),
            .parameterStatus(.init(parameter: "integer_datetimes", value: "on")),
            .parameterStatus(.init(parameter: "client_encoding", value: "UTF8")),
            .parameterStatus(.init(parameter: "TimeZone", value: "Etc/UTC")),
            .parameterStatus(.init(parameter: "is_superuser", value: "on")),
            .parameterStatus(.init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)")),
            .parameterStatus(.init(parameter: "session_authorization", value: "postgres")),
            .parameterStatus(.init(parameter: "IntervalStyle", value: "postgres")),
            .parameterStatus(.init(parameter: "standard_conforming_strings", value: "on")),
            .backendKeyData(.init(processID: 1234, secretKey: 5678))
        ]
        
        expected.forEach { message in
            switch message {
            case .parameterStatus(let parameterStatus):
                buffer.writeBackendMessage(id: .parameterStatus) { buffer in
                    buffer.writeNullTerminatedString(parameterStatus.parameter)
                    buffer.writeNullTerminatedString(parameterStatus.value)
                }
            case .backendKeyData(let backendKeyData):
                buffer.writeBackendMessage(id: .backendKeyData) { buffer in
                    buffer.writeInteger(backendKeyData.processID)
                    buffer.writeInteger(backendKeyData.secretKey)
                }
            default:
                XCTFail("Unexpected message type")
            }
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) }))
    }
    
    func testDecodeFailureBecauseOfMissingNullTermination() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterStatus) { buffer in
            buffer.writeString("DateStyle")
            buffer.writeString("ISO, MDY")
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
    
    func testDecodeFailureBecauseOfMissingNullTerminationInValue() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .parameterStatus) { buffer in
            buffer.writeNullTerminatedString("DateStyle")
            buffer.writeString("ISO, MDY")
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
}
