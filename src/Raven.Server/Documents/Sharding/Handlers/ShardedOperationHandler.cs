using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOperationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/operations/next-operation-id", "GET")]
        public async Task GetNextOperationId()
        {
            var nextId = ServerStore.Operations.GetNextOperationId();

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteNextOperationIdAndNodeTag(nextId, Server.ServerStore.NodeTag);
                }
            }
        }

        [RavenShardedAction("/databases/*/operations/state", "GET")]
        public async Task State()
        {
            var id = GetLongQueryString("id");

            var state = ServerStore.Operations.GetOperation(id)?.State;

            if (state == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await InternalGetState(state, context);
            }
        }
    }
}
