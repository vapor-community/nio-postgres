import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class ReadyForQueryTests: XCTestCase {

    func testDecode() {
        var buffer = ByteBuffer()
        
        let states: [PSQLBackendMessage.TransactionState] = [
            .idle,
            .inFailedTransaction,
            .inTransaction,
        ]
        
        states.forEach { state in
            buffer.writeBackendMessage(id: .readyForQuery) { buffer in
                switch state {
                case .idle:
                    buffer.writeInteger(UInt8(ascii: "I"))
                case .inTransaction:
                    buffer.writeInteger(UInt8(ascii: "T"))
                case .inFailedTransaction:
                    buffer.writeInteger(UInt8(ascii: "E"))
                }
            }
        }
        
        let expected = states.map { state -> PSQLBackendMessage in
            .readyForQuery(state)
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) }))

    }
    
    func testDecodeInvalidLength() {
        var buffer = ByteBuffer()
        
        buffer.writeBackendMessage(id: .readyForQuery) { buffer in
            buffer.writeInteger(UInt8(ascii: "I"))
            buffer.writeInteger(UInt8(ascii: "I"))
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
    
    func testDecodeUnexpectedAscii() {
        var buffer = ByteBuffer()
        
        buffer.writeBackendMessage(id: .readyForQuery) { buffer in
            buffer.writeInteger(UInt8(ascii: "F"))
        }
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: true) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
    
    func testDebugDescription() {
        XCTAssertEqual(String(reflecting: PSQLBackendMessage.TransactionState.idle), ".idle")
        XCTAssertEqual(String(reflecting: PSQLBackendMessage.TransactionState.inTransaction), ".inTransaction")
        XCTAssertEqual(String(reflecting: PSQLBackendMessage.TransactionState.inFailedTransaction), ".inFailedTransaction")
    }
}
