using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Routing;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers
{
    public class QueueEtlConnectionHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/queue/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetTestQueueConnectionResult()
        {
            try
            {
                string url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
                string jsonConfig = await new StreamReader(HttpContext.Request.Body).ReadToEndAsync();
                var config = JsonConvert.DeserializeObject<QueueConfiguration>(jsonConfig);

                var kafkaProducer = QueueHelper.CreateKafkaClient(
                    new QueueConnectionString
                    {
                        KafkaSettings = new KafkaSettings()
                        {
                            Url = url, 
                            ConnectionOptions = config.Configuration,
                            UseRavenCertificate = config.UseRavenCertificate
                        }
                    }, Guid.NewGuid().ToString(), Logger);

                kafkaProducer.InitTransactions(TimeSpan.FromSeconds(60));
                kafkaProducer.AbortTransaction();

                DynamicJsonValue result = new()
                {
                    [nameof(NodeConnectionTestResult.Success)] = true, [nameof(NodeConnectionTestResult.TcpServerUrl)] = url,
                };

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                await using (AsyncBlittableJsonTextWriter writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
            catch (Exception ex)
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer,
                            new DynamicJsonValue
                            {
                                [nameof(NodeConnectionTestResult.Success)] = false,
                                [nameof(NodeConnectionTestResult.Error)] = ex.ToString()
                            });
                    }
                }
            }
        }
    }

    public class QueueConfiguration
    {
        public Dictionary<string, string> Configuration { get; set; }
        public bool UseRavenCertificate { get; set; }
        public string Provider { get; set; }
    }
}
