using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetOutgoingFailureStats : AbstractReplicationHandlerProcessorForGetOutgoingFailureStats<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetOutgoingFailureStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                var stats = new ReplicationOutgoingsFailurePreview
                {
                    OutgoingsFailureInfo = new Dictionary<ReplicationNode, ConnectionShutdownInfo>(RequestHandler.Database.ReplicationLoader.OutgoingFailureInfo)
                };
       
                context.Write(writer, stats.ToJson());
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<ReplicationOutgoingsFailurePreview> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
