extension PSQLFrontendMessage {
    
    struct Query: PayloadEncodable, Equatable {
        
        /// The query string itself.
        let value: String
        
        func encode(into buffer: inout ByteBuffer) {
            buffer.writeNullTerminatedString(value)
        }
    }
}
