using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class GetAllTaskErrorsCommand : RavenCommand<TaskErrors[]>
{
    public GetAllTaskErrorsCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/tasks/errors";

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
