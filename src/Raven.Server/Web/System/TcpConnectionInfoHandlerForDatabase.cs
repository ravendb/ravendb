using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class TcpConnectionInfoHandlerForDatabase : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/info/tcp", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = Server.ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl());
                context.Write(writer, output);
            }

            return Task.CompletedTask;
        }
    }
}
