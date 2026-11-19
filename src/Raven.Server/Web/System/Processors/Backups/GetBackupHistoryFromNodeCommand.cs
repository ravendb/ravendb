using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class GetBackupHistoryFromNodeCommand : RavenCommand<BlittableJsonReaderObject>
{
    private readonly string _pathAndQuery;

    public GetBackupHistoryFromNodeCommand(string nodeTag, string pathAndQuery)
    {
        SelectedNodeTag = nodeTag;
        _pathAndQuery = pathAndQuery;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}{_pathAndQuery}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }
}
