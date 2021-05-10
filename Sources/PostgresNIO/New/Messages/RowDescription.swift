extension PSQLBackendMessage {
    
    struct RowDescription: PayloadDecodable, Equatable {
        /// Specifies the object ID of the parameter data type.
        var columns: [Column]
        
        struct Column: Equatable {
            /// The field name.
            var name: String
            
            /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
            var tableOID: Int32
            
            /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
            var columnAttributeNumber: Int16
            
            /// The object ID of the field's data type.
            var dataType: PSQLDataType
            
            /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
            var dataTypeSize: Int16
            
            /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
            var dataTypeModifier: Int32
            
            /// The format being used for the field. Currently will text or binary. In a RowDescription returned
            /// from the statement variant of Describe, the format code is not yet known and will always be text.
            var format: PSQLFormat
        }
        
        static func decode(from buffer: inout ByteBuffer) throws -> Self {
            try PSQLBackendMessage.ensureAtLeastNBytesRemaining(2, in: buffer)
            let columnCount = buffer.readInteger(as: Int16.self)!
            
            guard columnCount >= 0 else {
                throw PartialDecodingError.integerMustBePositiveOrNull(columnCount)
            }
            
            var result = [Column]()
            result.reserveCapacity(Int(columnCount))
            
            for _ in 0..<columnCount {
                guard let name = buffer.readNullTerminatedString() else {
                    throw PartialDecodingError.fieldNotDecodable(type: String.self)
                }
                
                try PSQLBackendMessage.ensureAtLeastNBytesRemaining(18, in: buffer)
                
                let tableOID = buffer.readInteger(as: Int32.self)!
                let columnAttributeNumber = buffer.readInteger(as: Int16.self)!
                let dataType = PSQLDataType(rawValue: buffer.readInteger(as: Int32.self)!)
                let dataTypeSize = buffer.readInteger(as: Int16.self)!
                let dataTypeModifier = buffer.readInteger(as: Int32.self)!
                let formatCodeInt16 = buffer.readInteger(as: Int16.self)!
                
                guard let format = PSQLFormat(rawValue: formatCodeInt16) else {
                    throw PartialDecodingError.valueNotRawRepresentable(value: formatCodeInt16, asType: PSQLFormat.self)
                }
                
                let field = Column(
                    name: name,
                    tableOID: tableOID,
                    columnAttributeNumber: columnAttributeNumber,
                    dataType: dataType,
                    dataTypeSize: dataTypeSize,
                    dataTypeModifier: dataTypeModifier,
                    format: format)
                
                result.append(field)
            }
            
            return RowDescription(columns: result)
        }
    }
}
