using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public sealed class QueueEtlConnectionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/queue/kafka/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestKafkaConnectionResult()
        {
            using (var processor = new QueueEtlHandlerProcessorForTestKafkaConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/etl/queue/rabbitmq/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestRabbitMqConnectionResult()
        {
            using (var processor = new QueueEtlHandlerProcessorForTestRabbitMqConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }
        
        [RavenAction("/databases/*/admin/etl/queue/azurequeuestorage/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestAzureQueueStorageConnectionResult()
        {
            using (var processor = new QueueEtlHandlerProcessorForTestAzureQueueStorageConnection<DatabaseRequestHandler, DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }
    }
}
