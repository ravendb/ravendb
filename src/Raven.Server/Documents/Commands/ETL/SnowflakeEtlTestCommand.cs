using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class SnowflakeEtlTestCommand(DocumentConventions conventions, BlittableJsonReaderObject testScript) : RavenCommand
{
    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/etl/snowflake/test";

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteObject(testScript);
                }
            }, conventions)
        };

        return request;
    }
}
