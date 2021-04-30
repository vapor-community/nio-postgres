import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class PSQLBackendMessageTests: XCTestCase {
    
    // MARK: ID
    
    func testInitMessageIDWithBytes() {
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "R")), .authentication)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "K")), .backendKeyData)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "2")), .bindComplete)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "3")), .closeComplete)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "C")), .commandComplete)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "d")), .copyData)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "c")), .copyDone)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "G")), .copyInResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "H")), .copyOutResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "W")), .copyBothResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "D")), .dataRow)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "I")), .emptyQueryResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "E")), .error)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "V")), .functionCallResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "v")), .negotiateProtocolVersion)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "n")), .noData)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "N")), .noticeResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "A")), .notificationResponse)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "t")), .parameterDescription)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "S")), .parameterStatus)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "1")), .parseComplete)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "s")), .portalSuspended)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "Z")), .readyForQuery)
        XCTAssertEqual(PSQLBackendMessage.ID(rawValue: UInt8(ascii: "T")), .rowDescription)
        
        XCTAssertNil(PSQLBackendMessage.ID(rawValue: 0))
    }
    
    func testMessageIDHasCorrectRawValue() {
        XCTAssertEqual(PSQLBackendMessage.ID.authentication.rawValue, UInt8(ascii: "R"))
        XCTAssertEqual(PSQLBackendMessage.ID.backendKeyData.rawValue, UInt8(ascii: "K"))
        XCTAssertEqual(PSQLBackendMessage.ID.bindComplete.rawValue, UInt8(ascii: "2"))
        XCTAssertEqual(PSQLBackendMessage.ID.closeComplete.rawValue, UInt8(ascii: "3"))
        XCTAssertEqual(PSQLBackendMessage.ID.commandComplete.rawValue, UInt8(ascii: "C"))
        XCTAssertEqual(PSQLBackendMessage.ID.copyData.rawValue, UInt8(ascii: "d"))
        XCTAssertEqual(PSQLBackendMessage.ID.copyDone.rawValue, UInt8(ascii: "c"))
        XCTAssertEqual(PSQLBackendMessage.ID.copyInResponse.rawValue, UInt8(ascii: "G"))
        XCTAssertEqual(PSQLBackendMessage.ID.copyOutResponse.rawValue, UInt8(ascii: "H"))
        XCTAssertEqual(PSQLBackendMessage.ID.copyBothResponse.rawValue, UInt8(ascii: "W"))
        XCTAssertEqual(PSQLBackendMessage.ID.dataRow.rawValue, UInt8(ascii: "D"))
        XCTAssertEqual(PSQLBackendMessage.ID.emptyQueryResponse.rawValue, UInt8(ascii: "I"))
        XCTAssertEqual(PSQLBackendMessage.ID.error.rawValue, UInt8(ascii: "E"))
        XCTAssertEqual(PSQLBackendMessage.ID.functionCallResponse.rawValue, UInt8(ascii: "V"))
        XCTAssertEqual(PSQLBackendMessage.ID.negotiateProtocolVersion.rawValue, UInt8(ascii: "v"))
        XCTAssertEqual(PSQLBackendMessage.ID.noData.rawValue, UInt8(ascii: "n"))
        XCTAssertEqual(PSQLBackendMessage.ID.noticeResponse.rawValue, UInt8(ascii: "N"))
        XCTAssertEqual(PSQLBackendMessage.ID.notificationResponse.rawValue, UInt8(ascii: "A"))
        XCTAssertEqual(PSQLBackendMessage.ID.parameterDescription.rawValue, UInt8(ascii: "t"))
        XCTAssertEqual(PSQLBackendMessage.ID.parameterStatus.rawValue, UInt8(ascii: "S"))
        XCTAssertEqual(PSQLBackendMessage.ID.parseComplete.rawValue, UInt8(ascii: "1"))
        XCTAssertEqual(PSQLBackendMessage.ID.portalSuspended.rawValue, UInt8(ascii: "s"))
        XCTAssertEqual(PSQLBackendMessage.ID.readyForQuery.rawValue, UInt8(ascii: "Z"))
        XCTAssertEqual(PSQLBackendMessage.ID.rowDescription.rawValue, UInt8(ascii: "T"))
    }
    
    // MARK: Decoder
    
    func testSSLSupportedAsFirstByte() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "S"))
        
        var expectedMessages: [PSQLBackendMessage] = [.sslSupported]
        
        // we test tons of ParameterStatus messages after the SSLSupported message, since those are
        // also identified by an "S"
        let parameterStatus: [PSQLBackendMessage.ParameterStatus] = [
            .init(parameter: "DateStyle", value: "ISO, MDY"),
            .init(parameter: "application_name", value: ""),
            .init(parameter: "server_encoding", value: "UTF8"),
            .init(parameter: "integer_datetimes", value: "on"),
            .init(parameter: "client_encoding", value: "UTF8"),
            .init(parameter: "TimeZone", value: "Etc/UTC"),
            .init(parameter: "is_superuser", value: "on"),
            .init(parameter: "server_version", value: "13.1 (Debian 13.1-1.pgdg100+1)"),
            .init(parameter: "session_authorization", value: "postgres"),
            .init(parameter: "IntervalStyle", value: "postgres"),
            .init(parameter: "standard_conforming_strings", value: "on"),
        ]
        
        parameterStatus.forEach { parameterStatus in
            buffer.writeBackendMessage(id: .parameterStatus) { buffer in
                buffer.writeNullTerminatedString(parameterStatus.parameter)
                buffer.writeNullTerminatedString(parameterStatus.value)
            }
            
            expectedMessages.append(.parameterStatus(parameterStatus))
        }
        
        let handler = ByteToMessageHandler(PSQLBackendMessage.Decoder())
        let embedded = EmbeddedChannel(handler: handler)
        XCTAssertNoThrow(try embedded.writeInbound(buffer))
        
        for expected in expectedMessages {
            var message: PSQLOptimizedBackendMessage?
            XCTAssertNoThrow(message = try embedded.readInbound(as: PSQLOptimizedBackendMessage.self))
            XCTAssertEqual(message, PSQLOptimizedBackendMessage.pure(expected))
        }
    }

    func testSSLUnsupportedAsFirstByte() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "N"))
        
        // we test a NoticeResponse messages after the SSLUnupported message, since NoticeResponse
        // is identified by a "N"
        let fields: [PSQLBackendMessage.Field : String] = [
            .file: "auth.c",
            .routine: "auth_failed",
            .line: "334",
            .localizedSeverity: "FATAL",
            .sqlState: "28P01",
            .severity: "FATAL",
            .message: "password authentication failed for user \"postgre3\"",
        ]
        
        let expectedMessages: [PSQLBackendMessage] = [
            .sslUnsupported,
            .notice(.init(fields: fields))
        ]
        
        buffer.writeBackendMessage(id: .noticeResponse) { buffer in
            fields.forEach { (key, value) in
                buffer.writeInteger(key.rawValue, as: UInt8.self)
                buffer.writeNullTerminatedString(value)
            }
            buffer.writeInteger(0, as: UInt8.self) // signal done
        }
        
        let handler = ByteToMessageHandler(PSQLBackendMessage.Decoder())
        let embedded = EmbeddedChannel(handler: handler)
        XCTAssertNoThrow(try embedded.writeInbound(buffer))
        
        for expected in expectedMessages {
            var message: PSQLOptimizedBackendMessage?
            XCTAssertNoThrow(message = try embedded.readInbound(as: PSQLOptimizedBackendMessage.self))
            XCTAssertEqual(message, PSQLOptimizedBackendMessage.pure(expected) )
        }
    }

    func testPayloadsWithoutAssociatedValues() {
        let messageIDs: [PSQLBackendMessage.ID] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]
        
        var buffer = ByteBuffer()
        messageIDs.forEach { messageID in
            buffer.writeBackendMessage(id: messageID) { _ in }
        }
        
        let expected: [PSQLBackendMessage] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
    }
    
    func testPayloadsWithoutAssociatedValuesInvalidLength() {
        let messageIDs: [PSQLBackendMessage.ID] = [
            .bindComplete,
            .closeComplete,
            .emptyQueryResponse,
            .noData,
            .parseComplete,
            .portalSuspended
        ]
        
        for messageID in messageIDs {
            var buffer = ByteBuffer()
            buffer.writeBackendMessage(id: messageID) { buffer in
                buffer.writeInteger(UInt8(0))
            }
            
            XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(buffer, [])],
                decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) })) {
                XCTAssert($0 is PSQLBackendMessage.DecodingError)
            }
        }
    }
    
    func testDecodeCommandCompleteMessage() {
        let expected: [PSQLBackendMessage] = [
            .commandComplete("SELECT 100"),
            .commandComplete("INSERT 0 1"),
            .commandComplete("UPDATE 1"),
            .commandComplete("DELETE 1")
        ]
        
        var okBuffer = ByteBuffer()
        expected.forEach { message in
            guard case .commandComplete(let commandTag) = message else {
                return XCTFail("Programming error!")
            }
            
            okBuffer.writeBackendMessage(id: .commandComplete) { buffer in
                buffer.writeNullTerminatedString(commandTag)
            }
        }
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(okBuffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
        
        // test commandTag is not null terminated
        for message in expected {
            guard case .commandComplete(let commandTag) = message else {
                return XCTFail("Programming error!")
            }
            
            var failBuffer = ByteBuffer()
            failBuffer.writeBackendMessage(id: .commandComplete) { buffer in
                buffer.writeString(commandTag)
            }
            
            XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
                inputOutputPairs: [(failBuffer, [])],
                decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) })) {
                XCTAssert($0 is PSQLBackendMessage.DecodingError)
            }
        }
    }
    
    func testDecodeMessageWithUnknownMessageID() {
        var buffer = ByteBuffer()
        buffer.writeInteger(UInt8(ascii: "x"))
        buffer.writeInteger(Int32(4))
        
        XCTAssertThrowsError(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, [])],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) })) {
            XCTAssert($0 is PSQLBackendMessage.DecodingError)
        }
    }
    
    func testDebugDescription() {
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.ok))", ".authentication(.ok)")
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.kerberosV5))",
                       ".authentication(.kerberosV5)")
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.md5(salt: (0, 1, 2, 3))))",
                       ".authentication(.md5(salt: (0, 1, 2, 3)))")
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.plaintext))",
                       ".authentication(.plaintext)")
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.scmCredential))",
                       ".authentication(.scmCredential)")
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.gss))",
                       ".authentication(.gss)")
        XCTAssertEqual("\(PSQLBackendMessage.authentication(.sspi))",
                       ".authentication(.sspi)")
        
        XCTAssertEqual("\(PSQLBackendMessage.backendKeyData(.init(processID: 1234, secretKey: 4567)))",
                       ".backendKeyData(processID: 1234, secretKey: 4567)")
        
        XCTAssertEqual("\(PSQLBackendMessage.bindComplete)", ".bindComplete")
        XCTAssertEqual("\(PSQLBackendMessage.closeComplete)", ".closeComplete")
        XCTAssertEqual("\(PSQLBackendMessage.commandComplete("SELECT 123"))", #".commandComplete("SELECT 123")"#)
        XCTAssertEqual("\(PSQLBackendMessage.emptyQueryResponse)", ".emptyQueryResponse")
        XCTAssertEqual("\(PSQLBackendMessage.noData)", ".noData")
        XCTAssertEqual("\(PSQLBackendMessage.parseComplete)", ".parseComplete")
        XCTAssertEqual("\(PSQLBackendMessage.portalSuspended)", ".portalSuspended")
        
        XCTAssertEqual("\(PSQLBackendMessage.readyForQuery(.idle))", ".readyForQuery(.idle)")
        XCTAssertEqual("\(PSQLBackendMessage.readyForQuery(.inTransaction))",
                       ".readyForQuery(.inTransaction)")
        XCTAssertEqual("\(PSQLBackendMessage.readyForQuery(.inFailedTransaction))",
                       ".readyForQuery(.inFailedTransaction)")
        XCTAssertEqual("\(PSQLBackendMessage.sslSupported)", ".sslSupported")
        XCTAssertEqual("\(PSQLBackendMessage.sslUnsupported)", ".sslUnsupported")
    }
    
}
