using Raven.Server.Documents.Sharding.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Commands
{
    public class ShardedCommand : ShardedBaseCommand<BlittableJsonReaderObject>
    {
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }

        public ShardedCommand(ShardedDatabaseRequestHandler handler, Headers headers, BlittableJsonReaderObject content = null) : base(handler, headers, content)
        {
        }
    }
}
