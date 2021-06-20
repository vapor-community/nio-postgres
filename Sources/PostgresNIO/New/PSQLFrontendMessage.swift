import NIO

/// A wire message that is created by a Postgres client to be consumed by Postgres server.
///
/// All messages are defined in the official Postgres Documentation in the section
/// [Frontend/Backend Protocol – Message Formats](https://www.postgresql.org/docs/13/protocol-message-formats.html)
enum PSQLFrontendMessage {
    typealias PayloadEncodable = PSQLFrontendMessagePayloadEncodable
    
    case bind(Bind)
    case cancel(Cancel)
    case close(Close)
    case describe(Describe)
    case execute(Execute)
    case flush
    case parse(Parse)
    case password(Password)
    case query(Query)
    case saslInitialResponse(SASLInitialResponse)
    case saslResponse(SASLResponse)
    case sslRequest(SSLRequest)
    case sync
    case startup(Startup)
    case terminate
    
    enum ID {
        case bind
        case close
        case describe
        case execute
        case flush
        case parse
        case password
        case query
        case saslInitialResponse
        case saslResponse
        case sync
        case terminate
        
        var byte: UInt8 {
            switch self {
            case .bind:
                return UInt8(ascii: "B")
            case .close:
                return UInt8(ascii: "C")
            case .describe:
                return UInt8(ascii: "D")
            case .execute:
                return UInt8(ascii: "E")
            case .flush:
                return UInt8(ascii: "H")
            case .parse:
                return UInt8(ascii: "P")
            case .password:
                return UInt8(ascii: "p")
            case .query:
                return UInt8(ascii: "Q")
            case .saslInitialResponse:
                return UInt8(ascii: "p")
            case .saslResponse:
                return UInt8(ascii: "p")
            case .sync:
                return UInt8(ascii: "S")
            case .terminate:
                return UInt8(ascii: "X")
            }
        }
    }
}

extension PSQLFrontendMessage {
    
    var id: ID {
        switch self {
        case .bind:
            return .bind
        case .cancel:
            preconditionFailure("Cancel messages don't have an identifier")
        case .close:
            return .close
        case .describe:
            return .describe
        case .execute:
            return .execute
        case .flush:
            return .flush
        case .parse:
            return .parse
        case .password:
            return .password
        case .query:
            return .query
        case .saslInitialResponse:
            return .saslInitialResponse
        case .saslResponse:
            return .saslResponse
        case .sslRequest:
            preconditionFailure("SSL requests don't have an identifier")
        case .startup:
            preconditionFailure("Startup messages don't have an identifier")
        case .sync:
            return .sync
        case .terminate:
            return .terminate

        }
    }
}

extension PSQLFrontendMessage {
    struct Encoder: MessageToByteEncoder {
        typealias OutboundIn = PSQLFrontendMessage
        
        let jsonEncoder: PSQLJSONEncoder
        
        init(jsonEncoder: PSQLJSONEncoder) {
            self.jsonEncoder = jsonEncoder
        }
        
        func encode(data message: PSQLFrontendMessage, out buffer: inout ByteBuffer) throws {
            struct EmptyPayload: PayloadEncodable {
                func encode(into buffer: inout ByteBuffer) {}
            }
            
            func encode<Payload: PayloadEncodable>(_ payload: Payload, into buffer: inout ByteBuffer) {
                let startIndex = buffer.writerIndex
                buffer.writeInteger(Int32(0)) // placeholder for length
                payload.encode(into: &buffer)
                let length = Int32(buffer.writerIndex - startIndex)
                buffer.setInteger(length, at: startIndex)
            }
            
            switch message {
            case .bind(let bind):
                buffer.writeInteger(message.id.byte)
                let startIndex = buffer.writerIndex
                buffer.writeInteger(Int32(0)) // placeholder for length
                try bind.encode(into: &buffer, using: self.jsonEncoder)
                let length = Int32(buffer.writerIndex - startIndex)
                buffer.setInteger(length, at: startIndex)
                
            case .cancel(let cancel):
                // cancel requests don't have an identifier
                encode(cancel, into: &buffer)
            case .close(let close):
                buffer.writeFrontendMessageID(message.id)
                encode(close, into: &buffer)
            case .describe(let describe):
                buffer.writeFrontendMessageID(message.id)
                encode(describe, into: &buffer)
            case .execute(let execute):
                buffer.writeFrontendMessageID(message.id)
                encode(execute, into: &buffer)
            case .flush:
                buffer.writeFrontendMessageID(message.id)
                encode(EmptyPayload(), into: &buffer)
            case .parse(let parse):
                buffer.writeFrontendMessageID(message.id)
                encode(parse, into: &buffer)
            case .password(let password):
                buffer.writeFrontendMessageID(message.id)
                encode(password, into: &buffer)
            case .query(let query):
                buffer.writeFrontendMessageID(message.id)
                encode(query, into: &buffer)
            case .saslInitialResponse(let saslInitialResponse):
                buffer.writeFrontendMessageID(message.id)
                encode(saslInitialResponse, into: &buffer)
            case .saslResponse(let saslResponse):
                buffer.writeFrontendMessageID(message.id)
                encode(saslResponse, into: &buffer)
            case .sslRequest(let request):
                // sslRequests don't have an identifier
                encode(request, into: &buffer)
            case .startup(let startup):
                // startup requests don't have an identifier
                encode(startup, into: &buffer)
            case .sync:
                buffer.writeFrontendMessageID(message.id)
                encode(EmptyPayload(), into: &buffer)
            case .terminate:
                buffer.writeFrontendMessageID(message.id)
                encode(EmptyPayload(), into: &buffer)
            }
        }
    }
}

protocol PSQLFrontendMessagePayloadEncodable {
    func encode(into buffer: inout ByteBuffer)
}
