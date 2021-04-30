import class Foundation.JSONEncoder
@testable import PostgresNIO

extension ConnectionStateMachine.ConnectionAction: Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.read, read):
            return true
        case (.wait, .wait):
            return true
        case (.provideAuthenticationContext, .provideAuthenticationContext):
            return true
        case (.sendStartupMessage, sendStartupMessage):
            return true
        case (.sendSSLRequest, sendSSLRequest):
            return true
        case (.establishSSLConnection, establishSSLConnection):
            return true
        case (.closeConnectionAndCleanup(let lhs), .closeConnectionAndCleanup(let rhs)):
            return lhs == rhs
        case (.sendPasswordMessage(let lhsMethod, let lhsAuthContext), sendPasswordMessage(let rhsMethod, let rhsAuthContext)):
            return lhsMethod == rhsMethod && lhsAuthContext == rhsAuthContext
        case (.sendParseDescribeBindExecuteSync(let lquery, let lbinds), sendParseDescribeBindExecuteSync(let rquery, let rbinds)):
            guard lquery == rquery else {
                return false
            }

            guard lbinds.count == rbinds.count else {
                return false
            }

            var lhsIterator = lbinds.makeIterator()
            var rhsIterator = rbinds.makeIterator()

            for _ in 0..<lbinds.count {
                let lhs = lhsIterator.next()!
                let rhs = rhsIterator.next()!

                guard lhs.psqlType == rhs.psqlType else {
                    return false
                }
                
                var lhsbuffer = ByteBuffer()
                var rhsbuffer = ByteBuffer()
                let encodingContext = PSQLEncodingContext(jsonEncoder: JSONEncoder())
                
                do {
                    try lhs.encodeRaw(into: &lhsbuffer, context: encodingContext)
                    try rhs.encodeRaw(into: &rhsbuffer, context: encodingContext)
                } catch {
                    return false
                }
                
                guard lhsbuffer == rhsbuffer else {
                    return false
                }
            }
            
            return true
        case (.fireEventReadyForQuery, .fireEventReadyForQuery):
            return true
        
        case (.succeedQueryNoRowsComming(let lhsContext, let lhsCommandTag), .succeedQueryNoRowsComming(let rhsContext, let rhsCommandTag)):
            return lhsContext === rhsContext && lhsCommandTag == rhsCommandTag
        case (.succeedQuery(let lhsContext, let lhsRowDescription), .succeedQuery(let rhsContext, let rhsRowDescription)):
            return lhsContext === rhsContext && lhsRowDescription == rhsRowDescription
        case (.failQuery(let lhsContext, let lhsError, let lhsCleanupContext), .failQuery(let rhsContext, let rhsError, let rhsCleanupContext)):
            return lhsContext === rhsContext && lhsError == rhsError && lhsCleanupContext == rhsCleanupContext
        case (.forwardRows(let lhsRows), .forwardRows(let rhsRows)):
            return lhsRows == rhsRows
        case (.forwardStreamComplete(let lhsBuffer, let lhsCommandTag, let lhsRead), .forwardStreamComplete(let rhsBuffer, let rhsCommandTag, let rhsRead)):
            return lhsBuffer == rhsBuffer && lhsCommandTag == rhsCommandTag && lhsRead == rhsRead
        case (.sendParseDescribeSync(let lhsName, let lhsQuery), .sendParseDescribeSync(let rhsName, let rhsQuery)):
            return lhsName == rhsName && lhsQuery == rhsQuery
        case (.succeedPreparedStatementCreation(let lhsContext, let lhsRowDescription), .succeedPreparedStatementCreation(let rhsContext, let rhsRowDescription)):
            return lhsContext === rhsContext && lhsRowDescription == rhsRowDescription
        case (.fireChannelInactive, .fireChannelInactive):
            return true
        default:
            return false
        }
    }
}

extension ConnectionStateMachine.ConnectionAction.CleanUpContext: Equatable {
    public static func == (lhs: Self, rhs: Self) -> Bool {
        guard lhs.closePromise?.futureResult === rhs.closePromise?.futureResult else {
            return false
        }
        
        guard lhs.error == rhs.error else {
            return false
        }
        
        guard lhs.tasks == rhs.tasks else {
            return false
        }
        
        return true
    }
}

extension ConnectionStateMachine {
    static func readyForQuery(transactionState: PSQLBackendMessage.TransactionState = .idle) -> Self {
        let connectionContext = Self.createConnectionContext(transactionState: transactionState)
        return ConnectionStateMachine(.readyForQuery(connectionContext))
    }
    
    static func createConnectionContext(transactionState: PSQLBackendMessage.TransactionState = .idle) -> ConnectionContext {
        let paramaters = [
            "DateStyle": "ISO, MDY",
            "application_name": "",
            "server_encoding": "UTF8",
            "integer_datetimes": "on",
            "client_encoding": "UTF8",
            "TimeZone": "Etc/UTC",
            "is_superuser": "on",
            "server_version": "13.1 (Debian 13.1-1.pgdg100+1)",
            "session_authorization": "postgres",
            "IntervalStyle": "postgres",
            "standard_conforming_strings": "on"
        ]
        
        return ConnectionContext(
            processID: 2730,
            secretKey: 882037977,
            parameters: paramaters,
            transactionState: transactionState)
    }
}

extension PSQLError: Equatable {
    public static func == (lhs: PSQLError, rhs: PSQLError) -> Bool {
        return true
    }
}

extension PSQLTask: Equatable {
    public static func == (lhs: PSQLTask, rhs: PSQLTask) -> Bool {
        switch (lhs, rhs) {
        case (.extendedQuery(let lhs), .extendedQuery(let rhs)):
            return lhs === rhs
        case (.preparedStatement(let lhs), .preparedStatement(let rhs)):
            return lhs === rhs
        case (.closeCommand(let lhs), .closeCommand(let rhs)):
            return lhs === rhs
        default:
            return false
        }
    }
}
