import NIO
import NIOTestUtils
import XCTest
@testable import PostgresNIO

class DataRowTests: XCTestCase {
    func testDecode() {
        let buffer = ByteBuffer.backendMessage(id: .dataRow) { buffer in
            // the data row has 3 columns
            buffer.writeInteger(3, as: Int16.self)
            
            // this is a null value
            buffer.writeInteger(-1, as: Int32.self)
            
            // this is an empty value. for example a empty string
            buffer.writeInteger(0, as: Int32.self)
            
            // this is a column with ten bytes
            buffer.writeInteger(10, as: Int32.self)
            buffer.writeBytes([UInt8](repeating: 5, count: 10))
        }

        let expectedColumns: [ByteBuffer?] = [
            nil,
            ByteBuffer(),
            ByteBuffer(bytes: [UInt8](repeating: 5, count: 10))
        ]
        
        let expectedInOuts: [(ByteBuffer, [PSQLOptimizedBackendMessage])] = [
            (buffer, [.dataRows(.init([.init(columns: expectedColumns)]))]),
        ]
        
        #warning("this needs a fix")
//        XCTAssertNoThrow(try ByteToMessageDecoderVerifier.verifyDecoder(
//            inputOutputPairs: expectedInOuts,
//            decoderFactory: { PSQLBackendMessage.Decoder(hasAlreadyReceivedBytes: false) }))
    }
}
