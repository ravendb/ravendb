using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetActiveConnections : AbstractReplicationHandlerProcessorForGetActiveConnections<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetActiveConnections([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var incomings = new DynamicJsonArray();
                foreach (var item in RequestHandler.Database.ReplicationLoader.IncomingConnections)
                {
                    incomings.Add(item.ToJson());
                }

                var outgoings = new DynamicJsonArray();
                foreach (var item in RequestHandler.Database.ReplicationLoader.OutgoingConnections)
                {
                    outgoings.Add(ReplicationActiveConnectionsPreview.OutgoingConnectionInfo.ToJson(item));
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(ReplicationActiveConnectionsPreview.IncomingConnections)] = incomings,
                    [nameof(ReplicationActiveConnectionsPreview.OutgoingConnections)] = outgoings
                });
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationActiveConnectionsPreview> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
