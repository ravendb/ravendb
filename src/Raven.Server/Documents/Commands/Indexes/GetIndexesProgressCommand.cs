using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

internal sealed class GetIndexesProgressCommand : RavenCommand<IndexProgress[]>
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
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            ThrowInvalidResponse();
            return; // never hit
        }

        Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexesProgress>(response).Results;
    }

    public override bool IsReadRequest => true;
}
