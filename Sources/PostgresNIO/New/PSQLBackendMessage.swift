import struct Foundation.Data


/// A protocol to implement for all associated value in the `PSQLBackendMessage` enum
protocol PSQLMessagePayloadDecodable {
    
    /// Decodes the associated value for a `PSQLBackendMessage` from the given `ByteBuffer`.
    ///
    /// When the decoding is done all bytes in the given `ByteBuffer` must be consumed.
    /// `buffer.readableBytes` must be `0`. In case of an error a `PartialDecodingError`
    /// must be thrown.
    ///
    /// - Parameter buffer: The `ByteBuffer` to read the message from. When done the `ByteBuffer`
    ///                     must be fully consumed.
    static func decode(from buffer: inout ByteBuffer) throws -> Self
}

/// A wire message that is created by a Postgres server to be consumed by Postgres client.
///
/// All messages are defined in the official Postgres Documentation in the section
/// [Frontend/Backend Protocol – Message Formats](https://www.postgresql.org/docs/13/protocol-message-formats.html)
enum PSQLBackendMessage {
    
    typealias PayloadDecodable = PSQLMessagePayloadDecodable
    
    case authentication(Authentication)
    case backendKeyData(BackendKeyData)
    case bindComplete
    case closeComplete
    case commandComplete(String)
    case dataRow(DataRow)
    case emptyQueryResponse
    case error(ErrorResponse)
    case noData
    case notice(NoticeResponse)
    case notification(NotificationResponse)
    case parameterDescription(ParameterDescription)
    case parameterStatus(ParameterStatus)
    case parseComplete
    case portalSuspended
    case readyForQuery(TransactionState)
    case rowDescription(RowDescription)
    case sslSupported
    case sslUnsupported
}

enum PSQLOptimizedBackendMessage {
    case pure(PSQLBackendMessage)
    case dataRows(CircularBuffer<PSQLBackendMessage.DataRow>)
}
    
extension PSQLBackendMessage {
    enum ID: RawRepresentable, Equatable {
        typealias RawValue = UInt8
        
        case authentication
        case backendKeyData
        case bindComplete
        case closeComplete
        case commandComplete
        case copyData
        case copyDone
        case copyInResponse
        case copyOutResponse
        case copyBothResponse
        case dataRow
        case emptyQueryResponse
        case error
        case functionCallResponse
        case negotiateProtocolVersion
        case noData
        case noticeResponse
        case notificationResponse
        case parameterDescription
        case parameterStatus
        case parseComplete
        case portalSuspended
        case readyForQuery
        case rowDescription
        
        init?(rawValue: UInt8) {
            switch rawValue {
            case UInt8(ascii: "R"):
                self = .authentication
            case UInt8(ascii: "K"):
                self = .backendKeyData
            case UInt8(ascii: "2"):
                self = .bindComplete
            case UInt8(ascii: "3"):
                self = .closeComplete
            case UInt8(ascii: "C"):
                self = .commandComplete
            case UInt8(ascii: "d"):
                self = .copyData
            case UInt8(ascii: "c"):
                self = .copyDone
            case UInt8(ascii: "G"):
                self = .copyInResponse
            case UInt8(ascii: "H"):
                self = .copyOutResponse
            case UInt8(ascii: "W"):
                self = .copyBothResponse
            case UInt8(ascii: "D"):
                self = .dataRow
            case UInt8(ascii: "I"):
                self = .emptyQueryResponse
            case UInt8(ascii: "E"):
                self = .error
            case UInt8(ascii: "V"):
                self = .functionCallResponse
            case UInt8(ascii: "v"):
                self = .negotiateProtocolVersion
            case UInt8(ascii: "n"):
                self = .noData
            case UInt8(ascii: "N"):
                self = .noticeResponse
            case UInt8(ascii: "A"):
                self = .notificationResponse
            case UInt8(ascii: "t"):
                self = .parameterDescription
            case UInt8(ascii: "S"):
                self = .parameterStatus
            case UInt8(ascii: "1"):
                self = .parseComplete
            case UInt8(ascii: "s"):
                self = .portalSuspended
            case UInt8(ascii: "Z"):
                self = .readyForQuery
            case UInt8(ascii: "T"):
                self = .rowDescription
            default:
                return nil
            }
        }
        
        var rawValue: UInt8 {
            switch self {
            case .authentication:
                return UInt8(ascii: "R")
            case .backendKeyData:
                return UInt8(ascii: "K")
            case .bindComplete:
                return UInt8(ascii: "2")
            case .closeComplete:
                return UInt8(ascii: "3")
            case .commandComplete:
                return UInt8(ascii: "C")
            case .copyData:
                return UInt8(ascii: "d")
            case .copyDone:
                return UInt8(ascii: "c")
            case .copyInResponse:
                return UInt8(ascii: "G")
            case .copyOutResponse:
                return UInt8(ascii: "H")
            case .copyBothResponse:
                return UInt8(ascii: "W")
            case .dataRow:
                return UInt8(ascii: "D")
            case .emptyQueryResponse:
                return UInt8(ascii: "I")
            case .error:
                return UInt8(ascii: "E")
            case .functionCallResponse:
                return UInt8(ascii: "V")
            case .negotiateProtocolVersion:
                return UInt8(ascii: "v")
            case .noData:
                return UInt8(ascii: "n")
            case .noticeResponse:
                return UInt8(ascii: "N")
            case .notificationResponse:
                return UInt8(ascii: "A")
            case .parameterDescription:
                return UInt8(ascii: "t")
            case .parameterStatus:
                return UInt8(ascii: "S")
            case .parseComplete:
                return UInt8(ascii: "1")
            case .portalSuspended:
                return UInt8(ascii: "s")
            case .readyForQuery:
                return UInt8(ascii: "Z")
            case .rowDescription:
                return UInt8(ascii: "T")
            }
        }
    }
}

extension PSQLBackendMessage {
    
    static func decode(from buffer: inout ByteBuffer, for messageID: ID) throws -> PSQLBackendMessage {
        switch messageID {
        case .authentication:
            return try .authentication(.decode(from: &buffer))
        case .backendKeyData:
            return try .backendKeyData(.decode(from: &buffer))
        case .bindComplete:
            try Self.ensureExactNBytesRemaining(0, in: buffer)
            return .bindComplete
        case .closeComplete:
            try Self.ensureExactNBytesRemaining(0, in: buffer)
            return .closeComplete
        case .commandComplete:
            guard let commandTag = buffer.readNullTerminatedString() else {
                throw PartialDecodingError.fieldNotDecodable(type: String.self)
            }
            return .commandComplete(commandTag)
        case .dataRow:
            return try .dataRow(.decode(from: &buffer))
        case .emptyQueryResponse:
            try Self.ensureExactNBytesRemaining(0, in: buffer)
            return .emptyQueryResponse
        case .parameterStatus:
            return try .parameterStatus(.decode(from: &buffer))
        case .error:
            return try .error(.decode(from: &buffer))
        case .noData:
            try Self.ensureExactNBytesRemaining(0, in: buffer)
            return .noData
        case .noticeResponse:
            return try .notice(.decode(from: &buffer))
        case .notificationResponse:
            return try .notification(.decode(from: &buffer))
        case .parameterDescription:
            return try .parameterDescription(.decode(from: &buffer))
        case .parseComplete:
            try Self.ensureExactNBytesRemaining(0, in: buffer)
            return .parseComplete
        case .portalSuspended:
            try Self.ensureExactNBytesRemaining(0, in: buffer)
            return .portalSuspended
        case .readyForQuery:
            return try .readyForQuery(.decode(from: &buffer))
        case .rowDescription:
            return try .rowDescription(.decode(from: &buffer))
        case .copyData, .copyDone, .copyInResponse, .copyOutResponse, .copyBothResponse, .functionCallResponse, .negotiateProtocolVersion:
            preconditionFailure()
        }
    }
}

extension PSQLBackendMessage {
    
    struct Decoder: ByteToMessageDecoder {
        typealias InboundOut = PSQLOptimizedBackendMessage
        
        private(set) var hasAlreadyReceivedBytes: Bool
        private(set) var dataRowBuffer = CircularBuffer<DataRow>()
        
        init(hasAlreadyReceivedBytes: Bool = false) {
            // just to not run into the first couple of resizes
            self.dataRowBuffer.reserveCapacity(32)
            self.hasAlreadyReceivedBytes = hasAlreadyReceivedBytes
        }
        
        mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            // make sure we have at least one byte to read
            guard buffer.readableBytes > 0 else {
                return .needMoreData
            }
            
            if !self.hasAlreadyReceivedBytes {
                // We have not received any bytes yet! Let's peek at the first message id. If it
                // is a "S" or "N" we assume that it is connected to an SSL upgrade request. All
                // other messages that we expect now, don't start with either "S" or "N"
                
                // we made sure, we have at least one byte available, above, thus force unwrap is okay
                let firstByte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self)!
                
                switch firstByte {
                case UInt8(ascii: "S"):
                    // mark byte as read
                    buffer.moveReaderIndex(forwardBy: 1)
                    context.fireChannelRead(wrapInboundOut(.pure(.sslSupported)))
                    self.hasAlreadyReceivedBytes = true
                    return .continue
                case UInt8(ascii: "N"):
                    // mark byte as read
                    buffer.moveReaderIndex(forwardBy: 1)
                    context.fireChannelRead(wrapInboundOut(.pure(.sslUnsupported)))
                    self.hasAlreadyReceivedBytes = true
                    return .continue
                default:
                    self.hasAlreadyReceivedBytes = true
                }
            }
            
            repeat {
                // all other packages have an Int32 after the identifier that determines their length.
                // do we have enough bytes for that?
                guard buffer.readableBytes >= 5 else {
                    return .needMoreData
                }
                
                let idByte = buffer.getInteger(at: buffer.readerIndex, as: UInt8.self)!
                let length = buffer.getInteger(at: buffer.readerIndex + 1, as: Int32.self)!
                
                guard length + 1 <= buffer.readableBytes else {
                    return .needMoreData
                }
                
                // At this point we are sure, that we have enough bytes to decode the next message.
                // 1. Create a byteBuffer that represents exactly the next message. This can be force
                //    unwrapped, since it was verified that enough bytes are available.
                let completeMessageBuffer = buffer.readSlice(length: 1 + Int(length))!
                
                // 2. make sure we have a known message identifier
                guard let messageID = PSQLBackendMessage.ID(rawValue: idByte) else {
                    throw DecodingError.unknownMessageIDReceived(messageID: idByte, messageBytes: completeMessageBuffer)
                }
                
                // 3. decode the message
                do {
                    // get a mutable byteBuffer copy
                    var slice = completeMessageBuffer
                    // move reader index forward by five bytes
                    slice.moveReaderIndex(forwardBy: 5)
                    
                    let message = try PSQLBackendMessage.decode(from: &slice, for: messageID)
                    
                    if case .dataRow(let row) = message {
                        self.dataRowBuffer.append(row)
                        continue
                    }
                    
                    if self.dataRowBuffer.count > 0 {
                        let localCopy = self.dataRowBuffer
                        // a malloc is cheaper than a malloc + copy
                        self.dataRowBuffer = .init(initialCapacity: self.dataRowBuffer.capacity)
                        context.fireChannelRead(wrapInboundOut(.dataRows(localCopy)))
                    }
                    context.fireChannelRead(wrapInboundOut(.pure(message)))
                    
                } catch let error as PartialDecodingError {
                    throw DecodingError.withPartialError(error, messageID: messageID, messageBytes: completeMessageBuffer)
                } catch {
                    preconditionFailure("Expected to only see `PartialDecodingError`s here.")
                }

            } while buffer.readableBytes >= 5
            
            if self.dataRowBuffer.count > 0 {
                let localCopy = self.dataRowBuffer
                // a malloc is cheaper than a malloc + copy
                self.dataRowBuffer = .init(initialCapacity: self.dataRowBuffer.capacity)
                context.fireChannelRead(wrapInboundOut(.dataRows(localCopy)))
            }
            
            if buffer.readableBytes == 0 {
                return .continue
            }
            
            return .needMoreData
        }
    }
}

extension PSQLBackendMessage: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .authentication(let authentication):
            return ".authentication(\(String(reflecting: authentication)))"
        case .backendKeyData(let backendKeyData):
            return ".backendKeyData(\(String(reflecting: backendKeyData)))"
        case .bindComplete:
            return ".bindComplete"
        case .closeComplete:
            return ".closeComplete"
        case .commandComplete(let commandTag):
            return ".commandComplete(\(String(reflecting: commandTag)))"
        case .dataRow(let dataRow):
            return ".dataRow(\(String(reflecting: dataRow)))"
        case .emptyQueryResponse:
            return ".emptyQueryResponse"
        case .error(let error):
            return ".error(\(String(reflecting: error)))"
        case .noData:
            return ".noData"
        case .notice(let notice):
            return ".notice(\(String(reflecting: notice)))"
        case .notification(let notification):
            return ".notification(\(String(reflecting: notification)))"
        case .parameterDescription(let parameterDescription):
            return ".parameterDescription(\(String(reflecting: parameterDescription)))"
        case .parameterStatus(let parameterStatus):
            return ".parameterStatus(\(String(reflecting: parameterStatus)))"
        case .parseComplete:
            return ".parseComplete"
        case .portalSuspended:
            return ".portalSuspended"
        case .readyForQuery(let transactionState):
            return ".readyForQuery(\(String(reflecting: transactionState)))"
        case .rowDescription(let rowDescription):
            return ".rowDescription(\(String(reflecting: rowDescription)))"
        case .sslSupported:
            return ".sslSupported"
        case .sslUnsupported:
            return ".sslUnsupported"
        }
    }
}

extension PSQLBackendMessage {
    
    /// An error representing a failure to decode [a Postgres wire message](https://www.postgresql.org/docs/13/protocol-message-formats.html)
    /// to the Swift structure `PSQLBackendMessage`.
    ///
    /// If you encounter a `DecodingError` when using a trusted Postgres server please make to file an issue at:
    /// [https://github.com/vapor/postgres-nio/issues](https://github.com/vapor/postgres-nio/issues)
    struct DecodingError: Error {
        
        /// The backend message ID bytes
        let messageID: UInt8
        
        /// The backend message's payload encoded in base64
        let payload: String
        
        /// A textual description of the error
        let description: String
        
        /// The file this error was thrown in
        let file: String
        
        /// The line in `file` this error was thrown
        let line: Int
        
        static func withPartialError(
            _ partialError: PartialDecodingError,
            messageID: PSQLBackendMessage.ID,
            messageBytes: ByteBuffer) -> Self
        {
            var byteBuffer = messageBytes
            let data = byteBuffer.readData(length: byteBuffer.readableBytes)!
            
            return DecodingError(
                messageID: messageID.rawValue,
                payload: data.base64EncodedString(),
                description: partialError.description,
                file: partialError.file,
                line: partialError.line)
        }
        
        static func unknownMessageIDReceived(
            messageID: UInt8,
            messageBytes: ByteBuffer,
            file: String = #file,
            line: Int = #line) -> Self
        {
            var byteBuffer = messageBytes
            let data = byteBuffer.readData(length: byteBuffer.readableBytes)!
            
            return DecodingError(
                messageID: messageID,
                payload: data.base64EncodedString(),
                description: "Received a message with messageID '\(Character(UnicodeScalar(messageID)))'. There is no message type associated with this message identifier.",
                file: file,
                line: line)
        }
        
    }

    struct PartialDecodingError: Error {
        /// A textual description of the error
        let description: String
        
        /// The file this error was thrown in
        let file: String
        
        /// The line in `file` this error was thrown
        let line: Int
        
        static func valueNotRawRepresentable<Target: RawRepresentable>(
            value: Target.RawValue,
            asType: Target.Type,
            file: String = #file,
            line: Int = #line) -> Self
        {
            return PartialDecodingError(
                description: "Can not represent '\(value)' with type '\(asType)'.",
                file: file, line: line)
        }
        
        static func unexpectedValue(value: Any, file: String = #file, line: Int = #line) -> Self {
            return PartialDecodingError(
                description: "Value '\(value)' is not expected.",
                file: file, line: line)
        }
        
        static func expectedAtLeastNRemainingBytes(_ expected: Int, actual: Int, file: String = #file, line: Int = #line) -> Self {
            return PartialDecodingError(
                description: "Expected at least '\(expected)' remaining bytes. But only found \(actual).",
                file: file, line: line)
        }
        
        static func expectedExactlyNRemainingBytes(_ expected: Int, actual: Int, file: String = #file, line: Int = #line) -> Self {
            return PartialDecodingError(
                description: "Expected exactly '\(expected)' remaining bytes. But found \(actual).",
                file: file, line: line)
        }
        
        static func fieldNotDecodable(type: Any.Type, file: String = #file, line: Int = #line) -> Self {
            return PartialDecodingError(
                description: "Could not read '\(type)' from ByteBuffer.",
                file: file, line: line)
        }
        
        static func integerMustBePositiveOrNull<Number: FixedWidthInteger>(_ actual: Number, file: String = #file, line: Int = #line) -> Self {
            return PartialDecodingError(
                description: "Expected the integer to be positive or null, but got \(actual).",
                file: file, line: line)
        }
    }
    
    @inline(__always)
    static func ensureAtLeastNBytesRemaining(_ n: Int, in buffer: ByteBuffer, file: String = #file, line: Int = #line) throws {
        guard buffer.readableBytes >= n else {
            throw PartialDecodingError.expectedAtLeastNRemainingBytes(2, actual: buffer.readableBytes, file: file, line: line)
        }
    }
    
    @inline(__always)
    static func ensureExactNBytesRemaining(_ n: Int, in buffer: ByteBuffer, file: String = #file, line: Int = #line) throws {
        guard buffer.readableBytes == n else {
            throw PartialDecodingError.expectedExactlyNRemainingBytes(n, actual: buffer.readableBytes, file: file, line: line)
        }
    }
}
