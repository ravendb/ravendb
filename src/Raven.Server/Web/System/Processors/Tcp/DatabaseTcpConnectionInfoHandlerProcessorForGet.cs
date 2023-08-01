using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Extensions;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Tcp;

internal sealed class DatabaseTcpConnectionInfoHandlerProcessorForGet<TOperationContext> : AbstractDatabaseHandlerProcessor<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    public DatabaseTcpConnectionInfoHandlerProcessorForGet([NotNull] AbstractDatabaseRequestHandler<TOperationContext> requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var forExternalUse = CanConnectViaPublicClusterTcpUrl() == false;

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            var output = ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl(), forExternalUse);
            context.Write(writer, output);
        }
    }

    private bool CanConnectViaPublicClusterTcpUrl()
    {
        var senderUrl = RequestHandler.GetStringQueryString("senderUrl", required: false);
        if (string.IsNullOrEmpty(senderUrl))
            return true;

        var clusterTopology = ServerStore.GetClusterTopology();
        var (hasUrl, _) = clusterTopology.TryGetNodeTagByUrl(senderUrl);
        return hasUrl;
    }
}
