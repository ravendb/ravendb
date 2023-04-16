using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Web.System;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.OngoingTasks;

internal class GetOngoingTasksCommand : RavenCommand<OngoingTasksResult>
{
    public GetOngoingTasksCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/tasks";

        var request = new HttpRequestMessage { Method = HttpMethod.Get };

        return request;
    }

    public override bool IsReadRequest => true;
}
