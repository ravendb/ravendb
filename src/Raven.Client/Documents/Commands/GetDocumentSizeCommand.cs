using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands;

internal class GetDocumentSizeCommand : RavenCommand<DocumentSizeDetails>
{
    private readonly string _id;
    public override bool IsReadRequest => true;

    public GetDocumentSizeCommand(string id)
    {
        _id = id;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/docs/size?id={Uri.EscapeDataString(_id)}";
        return new HttpRequestMessage
        {
            Method = HttpMethod.Get,
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<DocumentSizeDetails>(response);
    }
}
