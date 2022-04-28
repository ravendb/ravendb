using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.SampleData;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.SampleData
{
    internal class ShardedSampleDataHandlerProcessorForGetSampleDataClasses : AbstractSampleDataHandlerProcessorForGetSampleDataClasses<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedSampleDataHandlerProcessorForGetSampleDataClasses([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
