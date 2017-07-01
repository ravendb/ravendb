using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.System
{
    public class AdminConfigurationHandler : AdminRequestHandler
    {
        [RavenAction("/admin/configuration/client", "PUT")]
        public async Task PutClientConfiguration()
        {
            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var clientConfigurationJson = ctx.ReadForDisk(RequestBodyStream(), Constants.Configuration.ClientId);

                var clientConfiguration = JsonDeserializationServer.ClientConfiguration(clientConfigurationJson);
                var res = await ServerStore.PutValueInClusterAsync(new PutClientConfigurationCommand(clientConfiguration));
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);
                
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }
    }
}