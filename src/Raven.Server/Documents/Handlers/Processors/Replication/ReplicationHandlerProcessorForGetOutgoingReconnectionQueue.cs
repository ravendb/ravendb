using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

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
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                var reconnectionQueue = new ReplicationOutgoingReconnectionQueuePreview
                {
                    QueueInfo = RequestHandler.Database.ReplicationLoader.ReconnectQueue.ToList()
                };

                context.Write(writer, reconnectionQueue.ToJson());
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationOutgoingReconnectionQueuePreview> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
