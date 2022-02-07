using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedOperationHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/operations/next-operation-id", "GET")]
        public async Task GetNextOperationId()
        {
            var nextId = ServerStore.Operations.GetNextOperationId();

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Id");
                    writer.WriteInteger(nextId);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(GetNextOperationIdCommand.NodeTag));
                    writer.WriteString(Server.ServerStore.NodeTag);
                    writer.WriteEndObject();
                }
            }
        }
    }
}
