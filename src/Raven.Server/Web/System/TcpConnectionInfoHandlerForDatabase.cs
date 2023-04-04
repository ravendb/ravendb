using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class TcpConnectionInfoHandlerForDatabase : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/info/tcp", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task Get()
        {
            var forExternalUse = CanConnectViaPrivateTcpUrl() == false;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = Server.ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl(), forExternalUse);
                context.Write(writer, output);
            }
        }

        private bool CanConnectViaPrivateTcpUrl()
        {
            var senderUrl = GetStringQueryString("senderUrl", false);
            if (string.IsNullOrEmpty(senderUrl))
                return true;

            var clusterTopology = ServerStore.GetClusterTopology();
            var (hasUrl, _) = clusterTopology.TryGetNodeTagByUrl(senderUrl);
            return hasUrl;
        }
    }
}
