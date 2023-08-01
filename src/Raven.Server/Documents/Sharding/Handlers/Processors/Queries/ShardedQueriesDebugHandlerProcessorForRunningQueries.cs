using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Debugging.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal sealed class ShardedQueriesDebugHandlerProcessorForRunningQueries : AbstractQueriesDebugHandlerProcessorForRunningQueries<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedQueriesDebugHandlerProcessorForRunningQueries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractQueryRunner GetQueryRunner() => RequestHandler.DatabaseContext.QueryRunner;
}
