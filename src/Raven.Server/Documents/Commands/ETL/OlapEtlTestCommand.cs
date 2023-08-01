using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class OlapEtlTestCommand : RavenCommand
{
    private readonly DocumentConventions _conventions;
    private readonly BlittableJsonReaderObject _testScript;
    public override bool IsReadRequest => true;

    public OlapEtlTestCommand(DocumentConventions conventions, BlittableJsonReaderObject testScript)
    {
        _conventions = conventions;
        _testScript = testScript;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/etl/olap/test";

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteObject(_testScript);
                }
            }, _conventions)
        };

        return request;
    }
}

