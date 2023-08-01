using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Debugging.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal sealed class ShardedQueriesDebugHandlerProcessorForKillQuery : AbstractQueriesDebugHandlerProcessorForKillQuery<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedQueriesDebugHandlerProcessorForKillQuery([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractQueryRunner GetQueryRunner() => RequestHandler.DatabaseContext.QueryRunner;
}
