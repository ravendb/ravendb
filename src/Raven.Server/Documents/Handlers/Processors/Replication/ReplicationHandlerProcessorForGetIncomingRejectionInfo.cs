using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetIncomingRejectionInfo : AbstractReplicationHandlerProcessorForGetIncomingRejectionInfo<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetIncomingRejectionInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                var stats = new DynamicJsonArray();
                foreach (var statItem in RequestHandler.Database.ReplicationLoader.IncomingRejectionStats)
                {
                    stats.Add(new DynamicJsonValue
                    {
                        ["Key"] = statItem.Key.ToJson(),
                        ["Value"] = new DynamicJsonArray(statItem.Value.Select(x => new DynamicJsonValue
                        {
                            ["Reason"] = x.Reason,
                            ["When"] = x.When
                        }))
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = stats
                });
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
