using System;
using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class GetTaskErrorsCommand : RavenCommand<TaskErrors[]>
{
    private readonly string[] _names;
    private readonly TaskCategory _taskCategory;

    public GetTaskErrorsCommand(string[] names, TaskCategory taskCategory, string nodeTag)
    {
        _names = names;
        _taskCategory = taskCategory;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var endpoint = _taskCategory switch
        {
            TaskCategory.Etl => "etl/errors",
            TaskCategory.Ai => "ai/errors",
            _ => throw new ArgumentOutOfRangeException(nameof(_taskCategory), _taskCategory, null)
        };

        url = $"{node.Url}/databases/{node.Database}/{endpoint}";

        if (_names is { Length: > 0 })
        {
            foreach (var name in _names)
            {
                url = QueryHelpers.AddQueryString(url, "name", name);
            }
        }

        return new HttpRequestMessage { Method = HttpMethod.Get };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<TaskErrorsResponse>(response).Results;
    }

    // ReSharper disable once ClassNeverInstantiated.Local
    private sealed class TaskErrorsResponse
    {
        public TaskErrors[] Results { get; set; }
    }
}
