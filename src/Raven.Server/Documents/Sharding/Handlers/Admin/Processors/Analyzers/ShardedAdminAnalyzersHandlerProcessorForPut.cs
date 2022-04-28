using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Analyzers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Admin.Processors.Analyzers
{
    internal class ShardedAdminAnalyzersHandlerProcessorForPut : AbstractAdminAnalyzersHandlerProcessorForPut<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAdminAnalyzersHandlerProcessorForPut([NotNull] ShardedDatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }
    }
}
