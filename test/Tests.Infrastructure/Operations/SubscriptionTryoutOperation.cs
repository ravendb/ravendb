using System.IO;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Tests.Infrastructure.Operations;

public class SubscriptionTryoutOperation : RavenCommand<string>, IOperation<string>
{
    private readonly SubscriptionTryout _tryout;

    internal SubscriptionTryoutOperation(SubscriptionTryout tryout)
    {
        _tryout = tryout;
        ResponseType = RavenCommandResponseType.Raw;
    }

    public RavenCommand<string> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
    {
        return this;
    }

    public override bool IsReadRequest { get; } = false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, stream))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(SubscriptionTryout.ChangeVector));
                    writer.WriteString(_tryout.ChangeVector);
                    writer.WritePropertyName(nameof(SubscriptionTryout.Query));
                    writer.WriteString(_tryout.Query);
                    writer.WriteEndObject();
                }
            }, DocumentConventions.Default)
        };

        var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/subscriptions/try?pageSize=10");

        url = sb.ToString();

        return request;
    }

    public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
    {
        Result = new StreamReader(stream).ReadToEnd();
    }
}
