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
    internal class ReplicationHandlerProcessorForGetOutgoingFailureStats : AbstractReplicationHandlerProcessorForGetOutgoingFailureStats<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetOutgoingFailureStats([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in RequestHandler.Database.ReplicationLoader.OutgoingFailureInfo)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            [nameof(item.Key)] = item.Key.GetType().ToString(),
                            [nameof(item.Key.Url)] = item.Key.Url,
                            [nameof(item.Key.Database)] = item.Key.Database,
                            [nameof(item.Key.Disabled)] = item.Key.Disabled
                        },
                        ["Value"] = new DynamicJsonValue
                        {
                            ["ErrorsCount"] = item.Value.Errors.Count,
                            [nameof(item.Value.Errors)] = new DynamicJsonArray(item.Value.Errors.Select(e => e.ToString())),
                            [nameof(item.Value.NextTimeout)] = item.Value.NextTimeout,
                            [nameof(item.Value.RetryOn)] = item.Value.RetryOn,
                            [nameof(item.Value.DestinationDbId)] = item.Value.DestinationDbId,
                            [nameof(item.Value.LastHeartbeatTicks)] = item.Value.LastHeartbeatTicks,
                        }
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = data
                });
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
