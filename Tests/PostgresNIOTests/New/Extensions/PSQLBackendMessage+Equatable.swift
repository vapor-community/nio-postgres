@testable import PostgresNIO

extension PSQLBackendMessage: Equatable {
    
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.authentication(let lhs), .authentication(let rhs)):
            return lhs == rhs
        case (.backendKeyData(let lhs), .backendKeyData(let rhs)):
            return lhs == rhs
        case (.bindComplete, bindComplete):
            return true
        case (.closeComplete, closeComplete):
            return true
        case (.commandComplete(let lhs), commandComplete(let rhs)):
            return lhs == rhs
        case (.dataRow(let lhs), dataRow(let rhs)):
            return lhs == rhs
        case (.emptyQueryResponse, emptyQueryResponse):
            return true
        case (.error(let lhs), error(let rhs)):
            return lhs == rhs
        case (.noData, noData):
            return true
        case (.notice(let lhs), notice(let rhs)):
            return lhs == rhs
        case (.notification(let lhs), .notification(let rhs)):
            return lhs == rhs
        case (.parameterDescription(let lhs), parameterDescription(let rhs)):
            return lhs == rhs
        case (.parameterStatus(let lhs), parameterStatus(let rhs)):
            return lhs == rhs
        case (.parseComplete, parseComplete):
            return true
        case (.portalSuspended, portalSuspended):
            return true
        case (.readyForQuery(let lhs), readyForQuery(let rhs)):
            return lhs == rhs
        case (.rowDescription(let lhs), rowDescription(let rhs)):
            return lhs == rhs
        case (.sslSupported, sslSupported):
            return true
        case (.sslUnsupported, sslUnsupported):
            return true
        default:
            return false
        }
    }
}

extension PSQLOptimizedBackendMessage: Equatable {
    
    public static func ==(lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.pure(let lhs), .pure(let rhs)):
            return lhs == rhs
        case (.dataRows(let lhs), .dataRows(let rhs)):
            return lhs == rhs
        default:
            return false
        }
    }
    
}
