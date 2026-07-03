using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.ETL;
using Sparrow.Json;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class DeleteTaskErrorsCommand : RavenCommand
{
    private readonly TaskCategory _taskCategory;
    private readonly StringValues _names;

    public DeleteTaskErrorsCommand(StringValues names, TaskCategory taskCategory, string nodeTag)
    {
        _taskCategory = taskCategory;
        _names = names;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/{_taskCategory.ErrorsEndpoint()}";

        foreach (var name in _names)
            url = QueryHelpers.AddQueryString(url, "name", name);

        return new HttpRequestMessage { Method = HttpMethod.Delete };
    }
}
