using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedQueueEtlConnectionHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/etl/queue/kafka/test-connection", "POST")]
        public async Task GetTestKafkaConnectionResult()
        {
            using (var processor = new QueueEtlHandlerProcessorForTestKafkaConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl/queue/rabbitmq/test-connection", "POST")]
        public async Task GetTestRabbitMqConnectionResult()
        {
            using (var processor = new QueueEtlHandlerProcessorForTestRabbitMqConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }
        
        [RavenShardedAction("/databases/*/admin/etl/queue/azurequeuestorage/test-connection", "POST")]
        public async Task GetTestAzureQueueStorageConnectionResult()
        {
            using (var processor = new QueueEtlHandlerProcessorForTestAzureQueueStorageConnection<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
