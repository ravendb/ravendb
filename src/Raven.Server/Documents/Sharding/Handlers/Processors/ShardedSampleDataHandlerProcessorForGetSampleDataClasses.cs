using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors
{
    internal class ShardedSampleDataHandlerProcessorForGetSampleDataClasses : AbstractSampleDataHandlerProcessorForGetSampleDataClasses<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedSampleDataHandlerProcessorForGetSampleDataClasses([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }
    }
}
