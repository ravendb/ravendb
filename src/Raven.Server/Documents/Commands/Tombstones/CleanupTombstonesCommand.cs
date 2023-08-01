using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Tombstones;

public sealed class CleanupTombstonesCommand : RavenCommand<CleanupTombstonesCommand.Response>
{
    public sealed class Response
    {
        public int Value { get; set; }
    }

    public CleanupTombstonesCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/tombstones/cleanup";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = JsonDeserializationServer.CleanupTombstonesResponse(response);
    }
}


