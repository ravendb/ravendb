using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetOutgoingReconnectionQueue : AbstractReplicationHandlerProcessorForGetOutgoingReconnectionQueue<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetOutgoingReconnectionQueue([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var queueItem in RequestHandler.Database.ReplicationLoader.ReconnectQueue)
                {
                    data.Add(ReplicationActiveConnectionsPreview.OutgoingConnectionInfo.ToJson(queueItem));
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Queue-Info"] = data
                });
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
