extension PSQLCodable where Self: RawRepresentable, RawValue: PSQLCodable {
    var psqlType: PSQLDataType {
        self.rawValue.psqlType
    }
    
    var psqlFormat: PSQLFormat {
        self.rawValue.psqlFormat
    }
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Self {
        guard let rawValue = try? RawValue.decode(from: &buffer, type: type, format: format, context: context),
              let selfValue = Self.init(rawValue: rawValue) else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        return selfValue
    }
    
    func encode(into byteBuffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        try rawValue.encode(into: &byteBuffer, context: context)
    }
}
