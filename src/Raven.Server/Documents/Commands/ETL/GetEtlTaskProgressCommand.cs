using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal class GetEtlTaskProgressCommand : RavenCommand<EtlTaskProgress[]>
{
    public GetEtlTaskProgressCommand(string nodeTag)
    {
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/etl/progress";

        var request = new HttpRequestMessage { Method = HttpMethod.Get };

        return request;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        Result = JsonDeserializationServer.EtlTaskProgressResponse(response).Results;
    }

    internal class EtlTaskProgressResponse
    {
        public EtlTaskProgress[] Results { get; set; }
    }
}
