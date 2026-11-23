using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class GetBackupHistoryFromNodeCommand : RavenCommand<BlittableJsonReaderObject>
{
    private readonly string _pathAndQuery;
    private readonly BlittableJsonReaderObject _body;

    public GetBackupHistoryFromNodeCommand(string nodeTag, string pathAndQuery, BlittableJsonReaderObject body = null)
    {
        SelectedNodeTag = nodeTag;
        _pathAndQuery = pathAndQuery;
        _body = body;
    }

    public override bool IsReadRequest => true;

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = response;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}{_pathAndQuery}";

        if (_body == null)
            return new HttpRequestMessage { Method = HttpMethod.Get };

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteObject(_body);
                }
            }, DocumentConventions.Default)
        };
    }
}
