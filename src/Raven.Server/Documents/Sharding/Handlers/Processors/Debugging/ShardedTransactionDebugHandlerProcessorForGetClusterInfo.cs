using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Debugging.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Debugging
{
    internal sealed class ShardedTransactionDebugHandlerProcessorForGetClusterInfo : AbstractTransactionDebugHandlerProcessorForGetClusterInfo<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTransactionDebugHandlerProcessorForGetClusterInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
