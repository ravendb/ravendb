using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal sealed class ReplicationHandlerProcessorForGetActiveConnections : AbstractReplicationHandlerProcessorForGetActiveConnections<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetActiveConnections([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var activeConnectionsJson = RequestHandler.Database.ReplicationLoader.GetActiveConnectionsAsJson();
                context.Write(writer, activeConnectionsJson);
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationActiveConnectionsPreview> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
