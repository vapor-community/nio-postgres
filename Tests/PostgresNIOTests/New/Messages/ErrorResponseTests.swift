import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class ErrorResponseTests: XCTestCase {
    
    func testDecode() {
        let fields: [PSQLBackendMessage.Field : String] = [
            .file: "auth.c",
            .routine: "auth_failed",
            .line: "334",
            .localizedSeverity: "FATAL",
            .sqlState: "28P01",
            .severity: "FATAL",
            .message: "password authentication failed for user \"postgre3\"",
        ]

        let buffer = ByteBuffer.backendMessage(id: .error) { buffer in
            fields.forEach { (key, value) in
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self) // signal done
        }
        
        let expectedInOuts: [(ByteBuffer, [PSQLOptimizedBackendMessage])] = [
            (buffer, [.pure(.error(.init(fields: fields)))]),
        ]
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: expectedInOuts,
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
    }
}
