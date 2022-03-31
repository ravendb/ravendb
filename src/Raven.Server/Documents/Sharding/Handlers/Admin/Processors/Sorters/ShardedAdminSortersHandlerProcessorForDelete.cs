using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Sorters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Sorters;

internal class ShardedAdminSortersHandlerProcessorForDelete : AbstractAdminSortersHandlerProcessorForDelete<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAdminSortersHandlerProcessorForDelete([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

    protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnShardsAsync(index);
}
