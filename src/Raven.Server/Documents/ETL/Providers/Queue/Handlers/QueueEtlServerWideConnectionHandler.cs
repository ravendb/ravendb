using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public sealed class QueueEtlServerWideConnectionHandler : ServerRequestHandler
    {
        [RavenAction("/admin/etl/queue/kafka/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task GetTestKafkaConnectionResult() => QueueEtlTestConnectionHelpers.TestKafkaAsync(this);

        [RavenAction("/admin/etl/queue/rabbitmq/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task GetTestRabbitMqConnectionResult() => QueueEtlTestConnectionHelpers.TestRabbitMqAsync(this);

        [RavenAction("/admin/etl/queue/azurequeuestorage/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task GetTestAzureQueueStorageConnectionResult() => QueueEtlTestConnectionHelpers.TestAzureQueueStorageAsync(this);

        [RavenAction("/admin/etl/queue/amazonsqs/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task GetTestAmazonSqsConnectionResult() => QueueEtlTestConnectionHelpers.TestAmazonSqsAsync(this);
    }
}
