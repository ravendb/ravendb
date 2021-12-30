using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Json.Serialization;
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

    public class ShardedQueryCommand : ShardedBaseCommand<QueryResult>
    {
        public ShardedQueryCommand(ShardedRequestHandler handler, BlittableJsonReaderObject content) : base(handler, ShardedCommands.Headers.None, content)
        {
            Headers["Missing-Includes"] = "true";
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.QueryResult(response);
        }
    }

    internal class ShardedPutIndexCommand : ShardedBaseCommand<PutIndexesResponse>
    {
        public ShardedPutIndexCommand(ShardedRequestHandler handler, BlittableJsonReaderObject content) : base(handler, ShardedCommands.Headers.None, content)
        {
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonDeserializationClient.PutIndexesResponse(response);
        }
    }
}
