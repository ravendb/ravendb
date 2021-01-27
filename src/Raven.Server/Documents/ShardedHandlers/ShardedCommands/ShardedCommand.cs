using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class ShardedCommand : ShardedBaseCommand<BlittableJsonReaderObject>
    {
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }

        public ShardedCommand(ShardedRequestHandler handler, Headers headers, BlittableJsonReaderObject content = null) : base(handler, headers, content)
        {
        }
    }
}
