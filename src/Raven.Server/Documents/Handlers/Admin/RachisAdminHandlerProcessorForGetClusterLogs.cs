using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin;

internal sealed class RachisAdminHandlerProcessorForGetClusterLogs : AbstractServerHandlerProxyReadProcessor<RaftDebugView>
{
    private const string DetailedParameter = "detailed";

    private readonly bool _detailed;
    private readonly int _take;
    private readonly long? _fromIndex;

    public RachisAdminHandlerProcessorForGetClusterLogs([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
        _fromIndex = requestHandler.GetLongQueryString("from", required: false);
        _take = requestHandler.GetPageSize(defaultPageSize: 1024);
        _detailed = requestHandler.GetBoolValueQueryString(DetailedParameter, required: false) ?? false;
    }

    protected override bool SupportsCurrentNode => true;
    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var debugView = ServerStore.Engine.DebugView();

        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        {
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            {
                debugView.PopulateLogs(context, _fromIndex, _take, _detailed);
                context.Write(writer, debugView.ToJson());
            }
        }
    }

    protected override ValueTask<RavenCommand<RaftDebugView>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context) => 
        ValueTask.FromResult<RavenCommand<RaftDebugView>>(new GetClusterLogsCommand(nodeTag, _fromIndex, _take, _detailed));

    protected override Task HandleRemoteNodeAsync(ProxyCommand<RaftDebugView> command, JsonOperationContext context, OperationCancelToken token) => 
        ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);

    private sealed class GetClusterLogsCommand : RavenCommand<RaftDebugView>
    {
        private readonly long? _fromIndex;
        private readonly int _take;
        private readonly bool _detailed;

        public GetClusterLogsCommand(string nodeTag, long? fromIndex, int take, bool detailed)
        {
            _fromIndex = fromIndex;
            _take = take;
            _detailed = detailed;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/cluster/log?from={_fromIndex}&{RequestHandler.PageSizeParameter}={_take}&{DetailedParameter}={_detailed}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    }
}
