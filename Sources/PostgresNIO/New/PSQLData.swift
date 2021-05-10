/// The format the postgres types are encoded in on the wire.
///
/// Currently there a two wire formats supported:
///  - text
///  - binary
///
/// In a `RowDescription` returned from the statement variant of `Describe`,
/// the format is not yet known and will always be `.text`.
enum PSQLFormat: Int16 {
    case text = 0
    case binary = 1
}

struct PSQLData: Equatable {
    
    @usableFromInline var bytes: ByteBuffer?
    @usableFromInline var dataType: PSQLDataType
    @usableFromInline var format: PSQLFormat
    
    /// use this only for testing
    init(bytes: ByteBuffer?, dataType: PSQLDataType, format: PSQLFormat) {
        self.bytes = bytes
        self.dataType = dataType
        self.format = format
    }
    
    @inlinable
    func decode<T: PSQLDecodable>(as: Optional<T>.Type, context: PSQLDecodingContext) throws -> T? {
        try self.decodeIfPresent(as: T.self, context: context)
    }
    
    @inlinable
    func decode<T: PSQLDecodable>(as type: T.Type, context: PSQLDecodingContext) throws -> T {
        switch self.bytes {
        case .none:
            throw PSQLCastingError.missingData(targetType: type, type: self.dataType, context: context)
        case .some(var buffer):
            return try T.decode(from: &buffer, type: self.dataType, format: self.format, context: context)
        }
    }
    
    @inlinable
    func decodeIfPresent<T: PSQLDecodable>(as: T.Type, context: PSQLDecodingContext) throws -> T? {
        switch self.bytes {
        case .none:
            return nil
        case .some(var buffer):
            return try T.decode(from: &buffer, type: self.dataType, format: self.format, context: context)
        }
    }
}

struct PSQLDataType: RawRepresentable, Equatable, CustomStringConvertible {
    typealias RawValue = Int32
    
    /// The raw data type code recognized by PostgreSQL.
    var rawValue: Int32
    
    /// `0`
    static let null = PSQLDataType(0)
    /// `16`
    static let bool = PSQLDataType(16)
    /// `17`
    static let bytea = PSQLDataType(17)
    /// `18`
    static let char = PSQLDataType(18)
    /// `19`
    static let name = PSQLDataType(19)
    /// `20`
    static let int8 = PSQLDataType(20)
    /// `21`
    static let int2 = PSQLDataType(21)
    /// `23`
    static let int4 = PSQLDataType(23)
    /// `24`
    static let regproc = PSQLDataType(24)
    /// `25`
    static let text = PSQLDataType(25)
    /// `26`
    static let oid = PSQLDataType(26)
    /// `114`
    static let json = PSQLDataType(114)
    /// `194` pg_node_tree
    static let pgNodeTree = PSQLDataType(194)
    /// `600`
    static let point = PSQLDataType(600)
    /// `700`
    static let float4 = PSQLDataType(700)
    /// `701`
    static let float8 = PSQLDataType(701)
    /// `790`
    static let money = PSQLDataType(790)
    /// `1000` _bool
    static let boolArray = PSQLDataType(1000)
    /// `1001` _bytea
    static let byteaArray = PSQLDataType(1001)
    /// `1002` _char
    static let charArray = PSQLDataType(1002)
    /// `1003` _name
    static let nameArray = PSQLDataType(1003)
    /// `1005` _int2
    static let int2Array = PSQLDataType(1005)
    /// `1007` _int4
    static let int4Array = PSQLDataType(1007)
    /// `1009` _text
    static let textArray = PSQLDataType(1009)
    /// `1015` _varchar
    static let varcharArray = PSQLDataType(1015)
    /// `1016` _int8
    static let int8Array = PSQLDataType(1016)
    /// `1017` _point
    static let pointArray = PSQLDataType(1017)
    /// `1021` _float4
    static let float4Array = PSQLDataType(1021)
    /// `1022` _float8
    static let float8Array = PSQLDataType(1022)
    /// `1034` _aclitem
    static let aclitemArray = PSQLDataType(1034)
    /// `1042`
    static let bpchar = PSQLDataType(1042)
    /// `1043`
    static let varchar = PSQLDataType(1043)
    /// `1082`
    static let date = PSQLDataType(1082)
    /// `1083`
    static let time = PSQLDataType(1083)
    /// `1114`
    static let timestamp = PSQLDataType(1114)
    /// `1115` _timestamp
    static let timestampArray = PSQLDataType(1115)
    /// `1184`
    static let timestamptz = PSQLDataType(1184)
    /// `1266`
    static let timetz = PSQLDataType(1266)
    /// `1700`
    static let numeric = PSQLDataType(1700)
    /// `2278`
    static let void = PSQLDataType(2278)
    /// `2950`
    static let uuid = PSQLDataType(2950)
    /// `2951` _uuid
    static let uuidArray = PSQLDataType(2951)
    /// `3802`
    static let jsonb = PSQLDataType(3802)
    /// `3807` _jsonb
    static let jsonbArray = PSQLDataType(3807)

    /// Returns `true` if the type's raw value is greater than `2^14`.
    /// This _appears_ to be true for all user-defined types, but I don't
    /// have any documentation to back this up.
    var isUserDefined: Bool {
        self.rawValue >= 1 << 14
    }
    
    init(_ rawValue: Int32) {
        self.rawValue = rawValue
    }

    init(rawValue: Int32) {
        self.init(rawValue)
    }
    
    /// Returns the known SQL name, if one exists.
    /// Note: This only supports a limited subset of all PSQL types and is meant for convenience only.
    var knownSQLName: String? {
        switch self {
        case .bool: return "BOOLEAN"
        case .bytea: return "BYTEA"
        case .char: return "CHAR"
        case .name: return "NAME"
        case .int8: return "BIGINT"
        case .int2: return "SMALLINT"
        case .int4: return "INTEGER"
        case .regproc: return "REGPROC"
        case .text: return "TEXT"
        case .oid: return "OID"
        case .json: return "JSON"
        case .pgNodeTree: return "PGNODETREE"
        case .point: return "POINT"
        case .float4: return "REAL"
        case .float8: return "DOUBLE PRECISION"
        case .money: return "MONEY"
        case .boolArray: return "BOOLEAN[]"
        case .byteaArray: return "BYTEA[]"
        case .charArray: return "CHAR[]"
        case .nameArray: return "NAME[]"
        case .int2Array: return "SMALLINT[]"
        case .int4Array: return "INTEGER[]"
        case .textArray: return "TEXT[]"
        case .varcharArray: return "VARCHAR[]"
        case .int8Array: return "BIGINT[]"
        case .pointArray: return "POINT[]"
        case .float4Array: return "REAL[]"
        case .float8Array: return "DOUBLE PRECISION[]"
        case .aclitemArray: return "ACLITEM[]"
        case .bpchar: return "BPCHAR"
        case .varchar: return "VARCHAR"
        case .date: return "DATE"
        case .time: return "TIME"
        case .timestamp: return "TIMESTAMP"
        case .timestamptz: return "TIMESTAMPTZ"
        case .timestampArray: return "TIMESTAMP[]"
        case .numeric: return "NUMERIC"
        case .void: return "VOID"
        case .uuid: return "UUID"
        case .uuidArray: return "UUID[]"
        case .jsonb: return "JSONB"
        case .jsonbArray: return "JSONB[]"
        default: return nil
        }
    }
    
    /// See `CustomStringConvertible`.
    var description: String {
        return self.knownSQLName ?? "UNKNOWN \(self.rawValue)"
    }
}

