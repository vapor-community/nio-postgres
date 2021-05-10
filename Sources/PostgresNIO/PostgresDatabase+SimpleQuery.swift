import NIO
import Logging

extension PostgresDatabase {
    public func simpleQuery(_ string: String) -> EventLoopFuture<[PostgresRow]> {
        return self.query(string, []).map { $0.rows }
    }
    
    public func simpleQuery(_ string: String, _ onRow: @escaping (PostgresRow) throws -> ()) -> EventLoopFuture<Void> {
        self.query(string, onRow: onRow)
    }
}
