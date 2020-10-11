using System.Net.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetNextOperationIdCommand : RavenCommand<long>
    {
        public string NodeTag { get; private set; }

        public override bool IsReadRequest => false; // disable caching
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/operations/next-operation-id";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            response.TryGet("Id", out long id);
            response.TryGet(nameof(OperationIdResult.OperationNodeTag), out string nodeTag);

            NodeTag = nodeTag;
            Result = id;
        }
    }
}
