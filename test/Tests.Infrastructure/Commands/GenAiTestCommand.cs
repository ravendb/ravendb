using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Tests.Infrastructure.Commands;

public class GenAiTestCommand : RavenCommand<BlittableJsonReaderObject>
{
    private readonly DocumentConventions _conventions;
    private readonly BlittableJsonReaderObject _testScript;
    public override bool IsReadRequest => true;

    public GenAiTestCommand(DocumentConventions conventions, BlittableJsonReaderObject testScript)
    {
        _conventions = conventions;
        _testScript = testScript;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/admin/ai/gen-ai/test";

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _testScript).ConfigureAwait(false), _conventions)
        };

        return request;
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = response;
    }
}
