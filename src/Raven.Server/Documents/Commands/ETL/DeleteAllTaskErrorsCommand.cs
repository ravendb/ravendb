using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class DeleteAllTaskErrorsCommand : RavenCommand
{
    public DeleteAllTaskErrorsCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/tasks/errors";

        return new HttpRequestMessage { Method = HttpMethod.Delete };
    }
}
