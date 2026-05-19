using System;
using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client.Http;
using Raven.Server.Documents.ETL;
using Sparrow.Json;

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
        var path = _taskCategory switch
        {
            TaskCategory.Etl => "etl/errors",
            TaskCategory.Ai => "ai/errors",
            _ => throw new ArgumentOutOfRangeException(nameof(_taskCategory), _taskCategory, "Unknown task type")
        };

        url = $"{node.Url}/databases/{node.Database}/{path}";

        foreach (var name in _names)
            url = QueryHelpers.AddQueryString(url, "name", name);

        return new HttpRequestMessage { Method = HttpMethod.Delete };
    }
}
