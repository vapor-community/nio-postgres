import NIO
import struct Foundation.UUID

/// A type, of which arrays can be encoded into and decoded from a postgres binary format
protocol PSQLArrayElement: PSQLCodable {
    static var psqlArrayType: PSQLDataType { get }
    static var psqlArrayElementType: PSQLDataType { get }
}

extension Bool: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .boolArray }
    static var psqlArrayElementType: PSQLDataType { .bool }
}

extension ByteBuffer: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .byteaArray }
    static var psqlArrayElementType: PSQLDataType { .bytea }
}

extension UInt8: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .charArray }
    static var psqlArrayElementType: PSQLDataType { .char }
}

extension Int16: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .int2Array }
    static var psqlArrayElementType: PSQLDataType { .int2 }
}

extension Int32: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .int4Array }
    static var psqlArrayElementType: PSQLDataType { .int4 }
}

extension Int64: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .int8Array }
    static var psqlArrayElementType: PSQLDataType { .int8 }
}

extension Int: PSQLArrayElement {
    #if (arch(i386) || arch(arm))
    static var psqlArrayType: PSQLDataType { .int4Array }
    static var psqlArrayElementType: PSQLDataType { .int4 }
    #else
    static var psqlArrayType: PSQLDataType { .int8Array }
    static var psqlArrayElementType: PSQLDataType { .int8 }
    #endif
}

extension Float: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .float4Array }
    static var psqlArrayElementType: PSQLDataType { .float4 }
}

extension Double: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .float8Array }
    static var psqlArrayElementType: PSQLDataType { .float8 }
}

extension String: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .textArray }
    static var psqlArrayElementType: PSQLDataType { .text }
}

extension UUID: PSQLArrayElement {
    static var psqlArrayType: PSQLDataType { .uuidArray }
    static var psqlArrayElementType: PSQLDataType { .uuid }
}

extension Array: PSQLEncodable where Element: PSQLArrayElement {
    var psqlType: PSQLDataType {
        Element.psqlArrayType
    }
    
    var psqlFormat: PSQLFormat {
        .binary
    }
    
    func encode(into buffer: inout ByteBuffer, context: PSQLEncodingContext) throws {
        // 0 if empty, 1 if not
        buffer.writeInteger(self.isEmpty ? 0 : 1, as: UInt32.self)
        // b
        buffer.writeInteger(0, as: Int32.self)
        // array element type
        buffer.writeInteger(Element.psqlArrayElementType.rawValue)

        // continue if the array is not empty
        guard !self.isEmpty else {
            return
        }
        
        // length of array
        buffer.writeInteger(numericCast(self.count), as: Int32.self)
        // dimensions
        buffer.writeInteger(1, as: Int32.self)

        try self.forEach { element in
            try element.encodeRaw(into: &buffer, context: context)
        }
    }
}

extension Array: PSQLDecodable where Element: PSQLArrayElement {
    
    static func decode(from buffer: inout ByteBuffer, type: PSQLDataType, format: PSQLFormat, context: PSQLDecodingContext) throws -> Array<Element> {
        guard case .binary = format else {
            // currently we only support decoding arrays in binary format.
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let isNotEmpty = buffer.readInteger(as: Int32.self), (0...1).contains(isNotEmpty) else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let b = buffer.readInteger(as: Int32.self), b == 0 else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let elementType = buffer.readInteger(as: PSQLDataType.self) else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard isNotEmpty == 1 else {
            return []
        }
        
        guard let expectedArrayCount = buffer.readInteger(as: Int32.self), expectedArrayCount > 0 else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        guard let dimensions = buffer.readInteger(as: Int32.self), dimensions == 1 else {
            throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
        }
        
        var result = Array<Element>()
        result.reserveCapacity(Int(expectedArrayCount))
        
        for _ in 0 ..< expectedArrayCount {
            guard let elementLength = buffer.readInteger(as: Int32.self) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            guard var elementBuffer = buffer.readSlice(length: numericCast(elementLength)) else {
                throw PSQLCastingError.failure(targetType: Self.self, type: type, postgresData: buffer, context: context)
            }
            
            let element = try Element.decode(from: &elementBuffer, type: elementType, format: format, context: context)
            
            result.append(element)
        }
        
        return result
    }
}

extension Array: PSQLCodable where Element: PSQLArrayElement {

}
