import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class AuthenticationTests: XCTestCase {
    
    func testDecodeAuthentication() {
        var expected = [PSQLBackendMessage]()
        var buffer = ByteBuffer()
        
        // add ok
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(0))
        }
        expected.append(.authentication(.ok))
        
        // add kerberos
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(2))
        }
        expected.append(.authentication(.kerberosV5))
        
        // add plaintext
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(3))
        }
        expected.append(.authentication(.plaintext))
        
        // add md5
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(5))
            buffer.writeInteger(UInt8(1))
            buffer.writeInteger(UInt8(2))
            buffer.writeInteger(UInt8(3))
            buffer.writeInteger(UInt8(4))
        }
        expected.append(.authentication(.md5(salt: (1, 2, 3, 4))))
        
        // add scm credential
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(6))
        }
        expected.append(.authentication(.scmCredential))
        
        // add gss
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(7))
        }
        expected.append(.authentication(.gss))
        
        // add sspi
        buffer.writeBackendMessage(id: .authentication) { buffer in
            buffer.writeInteger(Int32(9))
        }
        expected.append(.authentication(.sspi))
        
        
        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
            inputOutputPairs: [(buffer, expected.map { .pure($0) })],
            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
    }
}
