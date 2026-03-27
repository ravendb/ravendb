using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForAddCdcSink :
        AbstractOngoingTasksHandlerProcessorForAddCdcSink<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForAddCdcSink(
            [NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void AssertCanAddOrUpdateCdcSink(ref BlittableJsonReaderObject cdcSinkConfiguration)
        {
            throw new NotSupportedInShardingException("CDC Sinks are currently not supported in sharding");
        }
    }
}
