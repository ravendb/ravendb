using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForAddQueueSink :
        AbstractOngoingTasksHandlerProcessorForAddQueueSink<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForAddQueueSink(
            [NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void AssertCanAddOrUpdateQueueSink(ref BlittableJsonReaderObject etlConfiguration)
        {
            throw new NotSupportedInShardingException("Queue Sinks are currently not supported in sharding");
        }
    }
}
