using System.Net.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;
using Raven.Server.Documents.TasksErrors;

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
        url = $"{node.Url}/databases/{node.Database}/{_taskCategory.ErrorsEndpoint()}";

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
