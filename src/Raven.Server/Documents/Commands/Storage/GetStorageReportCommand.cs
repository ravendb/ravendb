using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Storage;

internal class GetStorageReportCommand : RavenCommand
{
    public GetStorageReportCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/debug/storage/report";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }
}
