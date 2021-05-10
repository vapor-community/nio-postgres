import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class NotificationResponseTests: XCTestCase {
    
    func testDecode() {
        let expected: [PSQLBackendMessage] = [
            .notification(.init(backendPID: 123, channel: "test", payload: "hello")),
            .notification(.init(backendPID: 123, channel: "test", payload: "world")),
            .notification(.init(backendPID: 123, channel: "foo", payload: "bar"))
        ]
        
        var buffer = ByteBuffer()
        expected.forEach { message in
            guard case .notification(let notification) = message else {
                return XCTFail("Expected only to get notifications here!")
            }
            
            buffer.writeBackendMessage(id: .notificationResponse) { buffer in
                buffer.writeInteger(notification.backendPID)
                buffer.writeNullTerminatedString(notification.channel)
                buffer.writeNullTerminatedString(notification.payload)
            }
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) }))
    }
    
    func testDecodeFailureBecauseOfMissingNullTermination() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .notificationResponse) { buffer in
            buffer.writeInteger(Int32(123))
            buffer.writeString("test")
            buffer.writeString("hello")
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
    
    func testDecodeFailureBecauseOfMissingNullTerminationInValue() {
        var buffer = ByteBuffer()
        buffer.writeBackendMessage(id: .notificationResponse) { buffer in
            buffer.writeInteger(Int32(123))
            buffer.writeNullTerminatedString("hello")
            buffer.writeString("world")
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
}
