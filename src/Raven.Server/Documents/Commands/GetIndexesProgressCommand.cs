using System.Net.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands;

internal class GetIndexesProgressCommand : RavenCommand<IndexProgress[]>
{
    public GetIndexesProgressCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/progress";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            ThrowInvalidResponse();
            return; // never hit
        }

        Result = JsonDeserializationServer.IndexesProgress(response).Results;
    }

    public override bool IsReadRequest => true;
}
