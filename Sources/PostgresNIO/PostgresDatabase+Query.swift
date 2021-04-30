import NIO
import Logging

extension PostgresDatabase {
    public func query(
        _ string: String,
        _ binds: [PostgresData] = []
    ) -> EventLoopFuture<PostgresQueryResult> {
        var result: PostgresQueryResult?
        let request = PostgresCommands.queryAll(query: string, binds: binds, onResult: { result = $0 })
        
        return self.send(request, logger: logger).map {
            result!
        }
    }

    public func query(
        _ string: String,
        _ binds: [PostgresData] = [],
        onMetadata: @escaping (PostgresQueryMetadata) -> () = { _ in },
        onRow: @escaping (PostgresRow) throws -> ()
    ) -> EventLoopFuture<Void> {
        let request = PostgresCommands.query(query: string, binds: binds, onMetadata: onMetadata, onRow: onRow)
        
        return self.send(request, logger: logger)
    }
}

public struct PostgresQueryResult {
    public let metadata: PostgresQueryMetadata
    public let rows: [PostgresRow]
}

extension PostgresQueryResult: Collection {
    public typealias Index = Int
    public typealias Element = PostgresRow

    public var startIndex: Int {
        self.rows.startIndex
    }

    public var endIndex: Int {
        self.rows.endIndex
    }

    public subscript(position: Int) -> PostgresRow {
        self.rows[position]
    }

    public func index(after i: Int) -> Int {
        self.rows.index(after: i)
    }
}

public struct PostgresQueryMetadata {
    public let command: String
    public var oid: Int?
    public var rows: Int?

    init?(string: String) {
        let parts = string.split(separator: " ")
        guard parts.count >= 1 else {
            return nil
        }
        switch parts[0] {
        case "INSERT":
            // INSERT oid rows
            guard parts.count == 3 else {
                return nil
            }
            self.command = .init(parts[0])
            self.oid = Int(parts[1])
            self.rows = Int(parts[2])
        case "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY":
            // <cmd> rows
            guard parts.count == 2 else {
                return nil
            }
            self.command = .init(parts[0])
            self.oid = nil
            self.rows = Int(parts[1])
        default:
            // <cmd>
            self.command = string
            self.oid = nil
            self.rows = nil
        }
    }
}
