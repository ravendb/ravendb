using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Analyzers
{
    internal class ShardedAdminAnalyzersHandlerProcessorForDelete : AbstractAdminAnalyzersHandlerProcessorForDelete<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminAnalyzersHandlerProcessorForDelete([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
    }
}
