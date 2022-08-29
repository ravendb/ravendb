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
                var incoming = new DynamicJsonArray();
                foreach (var item in RequestHandler.Database.ReplicationLoader.IncomingConnections)
                {
                    incoming.Add(new DynamicJsonValue
                    {
                        ["SourceDatabaseId"] = item.SourceDatabaseId,
                        ["SourceDatabaseName"] = item.SourceDatabaseName,
                        ["SourceMachineName"] = item.SourceMachineName,
                        ["SourceUrl"] = item.SourceUrl
                    });
                }

                var outgoing = new DynamicJsonArray();
                foreach (var item in RequestHandler.Database.ReplicationLoader.OutgoingConnections)
                {
                    outgoing.Add(new DynamicJsonValue
                    {
                        ["Url"] = item.Url,
                        ["Database"] = item.Database,
                        ["Disabled"] = item.Disabled
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(ReplicationActiveConnectionsPreview.IncomingConnections)] = incoming,
                    [nameof(ReplicationActiveConnectionsPreview.OutgoingConnections)] = outgoing
                });
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationActiveConnectionsPreview> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
