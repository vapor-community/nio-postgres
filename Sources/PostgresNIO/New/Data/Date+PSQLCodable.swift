import struct Foundation.Date

extension Date: PSQLCodable {
    var psqlType: PSQLDataType {
        .timestamptz
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        switch type {
        case .timestamp, .timestamptz:
            guard buffer.readableBytes == 8, let microseconds = buffer.readInteger(as: Int64.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            let seconds = Double(microseconds) / Double(_microsecondsPerSecond)
            return Date(timeInterval: seconds, since: _psqlDateStart)
        case .date:
            guard buffer.readableBytes == 4, let days = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            let seconds = Int64(days) * _secondsInDay
            return Date(timeInterval: Double(seconds), since: _psqlDateStart)
        default:
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
    }
    
    func encode(into buffer: inout ByteBuffer, context: PSQLEncodingContext) {
        let seconds = self.timeIntervalSince(Self._psqlDateStart) * Double(Self._microsecondsPerSecond)
        buffer.writeInteger(Int64(seconds))
    }
    
    // MARK: Private Constants
    
    private static let _microsecondsPerSecond: Int64 = 1_000_000
    private static let _secondsInDay: Int64 = 24 * 60 * 60
    
    /// values are stored as seconds before or after midnight 2000-01-01
    private static let _psqlDateStart = Date(timeIntervalSince1970: 946_684_800)
}

