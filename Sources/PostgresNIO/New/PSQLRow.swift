
/// `PSQLRow` represents a single row that was received from the Postgres Server.
struct PSQLRow {
    internal let lookupTable: [String: Int]
    internal let data: PSQLBackendMessage.DataRow
    
    internal let columns: [PSQLBackendMessage.RowDescription.Column]
    internal let jsonDecoder: PSQLJSONDecoder
    
    internal init(data: PSQLBackendMessage.DataRow, lookupTable: [String: Int], columns: [PSQLBackendMessage.RowDescription.Column], jsonDecoder: PSQLJSONDecoder) {
        self.data = data
        self.lookupTable = lookupTable
        self.columns = columns
        self.jsonDecoder = jsonDecoder
    }
    
    /// Access the raw Postgres data in the n-th column
    subscript(index: Int) -> PSQLData {
        PSQLData(bytes: self.data.columns[index], dataType: self.columns[index].dataType)
    }
    
    // TBD: Should this be optional?
    /// Access the raw Postgres data in the column indentified by name
    subscript(column columnName: String) -> PSQLData? {
        guard let index = self.lookupTable[columnName] else {
            return nil
        }
        
        return self[index]
    }
    
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column name to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable>(column: String, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        guard let index = self.lookupTable[column] else {
            preconditionFailure("A column '\(column)' does not exist.")
        }
        
        return try self.decode(column: index, as: type, file: file, line: line)
    }
    
    /// Access the data in the provided column and decode it into the target type.
    ///
    /// - Parameters:
    ///   - column: The column index to read the data from
    ///   - type: The type to decode the data into
    /// - Throws: The error of the decoding implementation. See also `PSQLDecodable` protocol for this.
    /// - Returns: The decoded value of Type T.
    func decode<T: PSQLDecodable>(column index: Int, as type: T.Type, file: String = #file, line: Int = #line) throws -> T {
        let column = self.columns[index]
        
        let decodingContext = PSQLDecodingContext(
            jsonDecoder: jsonDecoder,
            columnName: column.name,
            columnIndex: index,
            file: file,
            line: line)
        
        return try self[index].decode(as: T.self, context: decodingContext)
    }
}

