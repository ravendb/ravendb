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
            var senderUrl = HttpContext.Request.GetClientRequestedNodeUrl();
            var forExternalUse = CanConnectViaPrivateTcpUrl(senderUrl) == false;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = Server.ServerStore.GetTcpInfoAndCertificates(senderUrl, forExternalUse);
                context.Write(writer, output);
            }
        }

        private bool CanConnectViaPrivateTcpUrl(string senderUrl)
        {
            var clusterTopology = ServerStore.GetClusterTopology();
            foreach (var node in clusterTopology.AllNodes)
            {
                var url = clusterTopology.GetUrlFromTag(node.Key);
                if (string.Equals(url, senderUrl))
                    return true;
            }
            return false;
        }
    }
}
