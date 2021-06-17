import Logging
@testable import PostgresNIO
import XCTest
import NIOTestUtils

final class IntegrationTests: XCTestCase {

    func testConnectAndClose() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        XCTAssertNoThrow(try conn?.close().wait())
    }
    
    func testAuthenticationFailure() throws {
        // If the postgres server trusts every connection, it is really hard to create an
        // authentication failure.
        try XCTSkipIf(env("POSTGRES_HOST_AUTH_METHOD") == "trust")
        
        let config = PSQLConnection.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: 5432,
            username: env("POSTGRES_USER") ?? "postgres",
            database: env("POSTGRES_DB"),
            password: "wrong_password",
            tlsConfiguration: nil)
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        
        var logger = Logger.psqlTest
        logger.logLevel = .info
        
        var connection: PSQLConnection?
        XCTAssertThrowsError(connection = try PSQLConnection.connect(configuration: config, logger: logger, on: eventLoopGroup.next()).wait()) {
            XCTAssertTrue($0 is PSQLError)
        }
        
        // In case of a test failure the created connection must be closed.
        XCTAssertNoThrow(try connection?.close().wait())
    }
    
    func testQueryVersion() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("SELECT version()", logger: .psqlTest).wait())
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        var version: String?
        XCTAssertNoThrow(version = try row?.decode(column: 0, as: String.self))
        XCTAssertEqual(version?.contains("PostgreSQL"), true)
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testQuery10kItems() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("SELECT generate_series(1, 10000);", logger: .psqlTest).wait())
        
        var expected: Int64 = 1
        
        XCTAssertNoThrow(try rows?.onRow { row in
            let promise = eventLoop.makePromise(of: Void.self)
            
            func workaround() {
                var number: Int64?
                XCTAssertNoThrow(number = try row.decode(column: 0, as: Int64.self))
                XCTAssertEqual(number, expected)
                expected += 1
            }
            
            eventLoop.execute {
                workaround()
                promise.succeed(())
            }
            
            return promise.futureResult
        }.wait())
        
        XCTAssertEqual(expected, 10001)
    }
    
    func test1kRoundTrips() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        for _ in 0..<1_000 {
            var rows: PSQLRows?
            XCTAssertNoThrow(rows = try conn?.query("SELECT version()", logger: .psqlTest).wait())
            var row: PSQLRows.Row?
            XCTAssertNoThrow(row = try rows?.next().wait())
            var version: String?
            XCTAssertNoThrow(version = try row?.decode(column: 0, as: String.self))
            XCTAssertEqual(version?.contains("PostgreSQL"), true)
            XCTAssertNil(try rows?.next().wait())
        }
    }
    
    func testQuerySelectParameter() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("SELECT $1::TEXT as foo", ["hello"], logger: .psqlTest).wait())
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        var foo: String?
        XCTAssertNoThrow(foo = try row?.decode(column: 0, as: String.self))
        XCTAssertEqual(foo, "hello")
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testDecodeIntegers() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("""
        SELECT
            1::SMALLINT                   as smallint,
            -32767::SMALLINT              as smallint_min,
            32767::SMALLINT               as smallint_max,
            1::INT                        as int,
            -2147483647::INT              as int_min,
            2147483647::INT               as int_max,
            1::BIGINT                     as bigint,
            -9223372036854775807::BIGINT  as bigint_min,
            9223372036854775807::BIGINT   as bigint_max
        """, logger: .psqlTest).wait())
        
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        
        XCTAssertEqual(try row?.decode(column: "smallint", as: Int16.self), 1)
        XCTAssertEqual(try row?.decode(column: "smallint_min", as: Int16.self), -32_767)
        XCTAssertEqual(try row?.decode(column: "smallint_max", as: Int16.self), 32_767)
        XCTAssertEqual(try row?.decode(column: "int", as: Int32.self), 1)
        XCTAssertEqual(try row?.decode(column: "int_min", as: Int32.self), -2_147_483_647)
        XCTAssertEqual(try row?.decode(column: "int_max", as: Int32.self), 2_147_483_647)
        XCTAssertEqual(try row?.decode(column: "bigint", as: Int64.self), 1)
        XCTAssertEqual(try row?.decode(column: "bigint_min", as: Int64.self), -9_223_372_036_854_775_807)
        XCTAssertEqual(try row?.decode(column: "bigint_max", as: Int64.self), 9_223_372_036_854_775_807)
        
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testEncodeAndDecodeIntArray() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        let array: [Int64] = [1, 2, 3]
        XCTAssertNoThrow(rows = try conn?.query("SELECT $1::int8[] as array", [array], logger: .psqlTest).wait())
        
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        
        XCTAssertEqual(try row?.decode(column: "array", as: [Int64].self), array)
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testDecodeEmptyIntegerArray() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("SELECT '{}'::int[] as array", logger: .psqlTest).wait())
        
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        
        XCTAssertEqual(try row?.decode(column: "array", as: [Int64].self), [])
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testDoubleArraySerialization() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        let doubles: [Double] = [3.14, 42]
        XCTAssertNoThrow(rows = try conn?.query("SELECT $1::double precision[] as doubles", [doubles], logger: .psqlTest).wait())
        
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        
        XCTAssertEqual(try row?.decode(column: "doubles", as: [Double].self), doubles)
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testDecodeDates() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("""
            SELECT
                '2016-01-18 01:02:03 +0042'::DATE         as date,
                '2016-01-18 01:02:03 +0042'::TIMESTAMP    as timestamp,
                '2016-01-18 01:02:03 +0042'::TIMESTAMPTZ  as timestamptz
            """, logger: .psqlTest).wait())
        
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        
        XCTAssertEqual(try row?.decode(column: "date", as: Date.self).description, "2016-01-18 00:00:00 +0000")
        XCTAssertEqual(try row?.decode(column: "timestamp", as: Date.self).description, "2016-01-18 01:02:03 +0000")
        XCTAssertEqual(try row?.decode(column: "timestamptz", as: Date.self).description, "2016-01-18 00:20:03 +0000")
        
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testDecodeUUID() {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        var rows: PSQLRows?
        XCTAssertNoThrow(rows = try conn?.query("""
            SELECT '2c68f645-9ca6-468b-b193-ee97f241c2f8'::UUID as uuid
            """, logger: .psqlTest).wait())
        
        var row: PSQLRows.Row?
        XCTAssertNoThrow(row = try rows?.next().wait())
        
        XCTAssertEqual(try row?.decode(column: "uuid", as: UUID.self), UUID(uuidString: "2c68f645-9ca6-468b-b193-ee97f241c2f8"))
        
        XCTAssertNil(try rows?.next().wait())
    }
    
    func testRoundTripJSONB() {
        struct Object: Codable, PSQLCodable {
            let foo: Int
            let bar: Int
        }
        
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { XCTAssertNoThrow(try eventLoopGroup.syncShutdownGracefully()) }
        let eventLoop = eventLoopGroup.next()
        
        var conn: PSQLConnection?
        XCTAssertNoThrow(conn = try PSQLConnection.test(on: eventLoop).wait())
        defer { XCTAssertNoThrow(try conn?.close().wait()) }
        
        do {
            var rows: PSQLRows?
            XCTAssertNoThrow(rows = try conn?.query("""
                select $1::jsonb as jsonb
                """, [Object(foo: 1, bar: 2)], logger: .psqlTest).wait())
            
            var row: PSQLRows.Row?
            XCTAssertNoThrow(row = try rows?.next().wait())
            var result: Object?
            XCTAssertNoThrow(result = try row?.decode(column: "jsonb", as: Object.self))
            XCTAssertEqual(result?.foo, 1)
            XCTAssertEqual(result?.bar, 2)
            
            XCTAssertNil(try rows?.next().wait())
        }
        
        do {
            var rows: PSQLRows?
            XCTAssertNoThrow(rows = try conn?.query("""
                select $1::json as json
                """, [Object(foo: 1, bar: 2)], logger: .psqlTest).wait())
            
            var row: PSQLRows.Row?
            XCTAssertNoThrow(row = try rows?.next().wait())
            var result: Object?
            XCTAssertNoThrow(result = try row?.decode(column: "json", as: Object.self))
            XCTAssertEqual(result?.foo, 1)
            XCTAssertEqual(result?.bar, 2)
            
            XCTAssertNil(try rows?.next().wait())
        }
    }
}


extension PSQLConnection {
    
    static func test(on eventLoop: EventLoop, logLevel: Logger.Level = .info) -> EventLoopFuture<PSQLConnection> {
        var logger = Logger(label: "psql.connection.test")
        logger.logLevel = logLevel
        let config = PSQLConnection.Configuration(
            host: env("POSTGRES_HOSTNAME") ?? "localhost",
            port: 5432,
            username: env("POSTGRES_USER") ?? "postgres",
            database: env("POSTGRES_DB"),
            password: env("POSTGRES_PASSWORD"),
            tlsConfiguration: nil)
        
        return PSQLConnection.connect(configuration: config, logger: logger, on: eventLoop)
    }
    
}
