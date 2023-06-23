using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Stats;

internal class AdminStatsHandlerProcessorForGetServerStatistics : AbstractServerHandlerProxyReadProcessor<ServerStatistics>
{
    public AdminStatsHandlerProcessorForGetServerStatistics([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
        {
            ServerStore.Server.Statistics.WriteTo(writer);
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<ServerStatistics> command, JsonOperationContext context, OperationCancelToken token)
    {
        return ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    protected override ValueTask<RavenCommand<ServerStatistics>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        return ValueTask.FromResult<RavenCommand<ServerStatistics>>(new GetServerStatisticsCommand(nodeTag));
    }

    private class GetServerStatisticsCommand : RavenCommand<ServerStatistics>
    {
        public GetServerStatisticsCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/stats";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    }
}
