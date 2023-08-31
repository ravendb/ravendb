using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Analyzers
{
    internal sealed class ShardedAdminAnalyzersHandlerProcessorForDelete : AbstractAdminAnalyzersHandlerProcessorForDelete<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminAnalyzersHandlerProcessorForDelete([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
