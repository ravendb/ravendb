using System.Threading.Tasks;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedTcpConnectionInfoHandlerForDatabase : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/info/tcp", "GET")]
        public async Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = Server.ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl());
                context.Write(writer, output);
            }
        }
    }
}
