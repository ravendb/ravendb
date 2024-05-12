using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.AzureQueueStorage;
using Raven.Server.Documents.ETL.Providers.Queue.Kafka;
using Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForProgress : AbstractEtlHandlerProcessorForProgress<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForProgress([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
        using (context.OpenReadTransaction())
        {
            var names = GetNames();
            var performance = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names).Select(x => new EtlTaskProgress
            {
                TaskName = x.Key,
                EtlType = x.Value.First().EtlType,
                ProcessesProgress = x.Value.Select(y => y.GetProgress(context)).ToArray(),
                QueueBrokerType = x.Value.First() switch
                {
                    RabbitMqEtl => QueueBrokerType.RabbitMq,
                    KafkaEtl => QueueBrokerType.Kafka,
                    AzureQueueStorageEtl => QueueBrokerType.AzureQueueStorage,
                    _ => null
                }
            }).ToArray();

            writer.WriteEtlTaskProgress(context, performance);
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskProgress[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
