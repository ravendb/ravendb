using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers.ShardedCommands
{
    public class ShardedExportCommand : ShardedStreamCommand
    {
        internal long OperationID;
        public ShardedExportCommand(ShardedRequestHandler handler, Operations.Operations operation, Func<Stream, Task> handleStreamResponse, BlittableJsonReaderObject content) : 
            base(handler, handleStreamResponse, content)
        {
            OperationID = operation.GetNextOperationId();
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            Url = $"/smuggler/export?operationId={OperationID}";
            return base.CreateRequest(ctx, node, out url);
        }
    }
}
