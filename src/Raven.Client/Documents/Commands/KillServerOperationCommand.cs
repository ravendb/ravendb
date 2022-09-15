using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands;

internal class KillServerOperationCommand : RavenCommand
{
    private readonly long _id;

    public KillServerOperationCommand(long id)
    {
        _id = id;
    }

    public KillServerOperationCommand(long id, string nodeTag) : this(id)
    {
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/admin/operations/kill?id={_id}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }
}
