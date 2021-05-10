enum PSQLTask {
    case extendedQuery(ExtendedQueryContext)
    case preparedStatement(PrepareStatementContext)
    case closeCommand(CloseCommandContext)
    
    func failWithError(_ error: PSQLError) {
        switch self {
        case .extendedQuery(let extendedQueryContext):
            extendedQueryContext.promise.fail(error)
        case .preparedStatement(let createPreparedStatementContext):
            createPreparedStatementContext.promise.fail(error)
        case .closeCommand(let closeCommandContext):
            closeCommandContext.promise.fail(error)
        }
    }
}

final class ExtendedQueryContext {
    enum Query {
        case unnamed(String)
        case preparedStatement(name: String, rowDescription: PSQLBackendMessage.RowDescription?)
    }
    
    let query: Query
    let bind: [PSQLEncodable]
    let logger: Logger
    
    let jsonDecoder: PSQLJSONDecoder
    let promise: EventLoopPromise<PSQLRowBatchStream>
    
    init(query: String,
         bind: [PSQLEncodable],
         logger: Logger,
         jsonDecoder: PSQLJSONDecoder,
         promise: EventLoopPromise<PSQLRowBatchStream>)
    {
        self.query = .unnamed(query)
        self.bind = bind
        self.logger = logger
        self.jsonDecoder = jsonDecoder
        self.promise = promise
    }
    
    init(preparedStatement: PSQLPreparedStatement,
         bind: [PSQLEncodable],
         logger: Logger,
         jsonDecoder: PSQLJSONDecoder,
         promise: EventLoopPromise<PSQLRowBatchStream>)
    {
        self.query = .preparedStatement(
            name: preparedStatement.name,
            rowDescription: preparedStatement.rowDescription)
        self.bind = bind
        self.logger = logger
        self.jsonDecoder = jsonDecoder
        self.promise = promise
    }

}

final class PrepareStatementContext {
    let name: String
    let query: String
    let logger: Logger
    let promise: EventLoopPromise<PSQLBackendMessage.RowDescription?>
    
    init(name: String,
         query: String,
         logger: Logger,
         promise: EventLoopPromise<PSQLBackendMessage.RowDescription?>)
    {
        self.name = name
        self.query = query
        self.logger = logger
        self.promise = promise
    }
}

final class CloseCommandContext {
    
    let target: CloseTarget
    let logger: Logger
    let promise: EventLoopPromise<Void>
    
    init(target: CloseTarget,
         logger: Logger,
         promise: EventLoopPromise<Void>)
    {
        self.target = target
        self.logger = logger
        self.promise = promise
    }
}

