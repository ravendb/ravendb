using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

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
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var activeConnectionsPreview = new ReplicationActiveConnectionsPreview
                {
                    IncomingConnections = RequestHandler.Database.ReplicationLoader.IncomingConnections.ToList(),
                    OutgoingConnections = RequestHandler.Database.ReplicationLoader.OutgoingConnections.ToList()
                };

                context.Write(writer, activeConnectionsPreview.ToJson());
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationActiveConnectionsPreview> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
