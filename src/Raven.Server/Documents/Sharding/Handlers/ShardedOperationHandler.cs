using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Operations;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOperationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/operations/next-operation-id", "GET")]
        public async Task GetNextOperationId()
        {
            using (var processor = new ShardedOperationsHandlerProcessorForGetNextOperationId(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/operations/kill", "POST")]
        public async Task Kill()
        {
            using (var processor = new ShardedOperationsHandlerProcessorForKill(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/operations/state", "GET")]
        public async Task State()
        {
            var id = GetLongQueryString("id");

            var state = DatabaseContext.Operations.GetOperation(id)?.State;

            if (state == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await InternalGetStateAsync(state, context);
            }
        }
    }
}
