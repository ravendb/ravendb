using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands;

public sealed class GenerateClassFromDocumentCommand : RavenCommand<string>
{
    private readonly string _id;
    private readonly string _collection;
    private readonly string _lang;

    public GenerateClassFromDocumentCommand(string id, string collection, string lang)
    {
        if (string.IsNullOrEmpty(id) && string.IsNullOrEmpty(collection))
            throw new InvalidOperationException("Either id or collection must be provided.");

        _id = id;
        _collection = collection;
        _lang = lang ?? throw new ArgumentException(nameof(lang));
        ResponseType = RavenCommandResponseType.Raw;
    }

    public override bool IsReadRequest => true;
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/docs/class?id={Uri.EscapeDataString(_id)}&collection={Uri.EscapeDataString(_collection)}&lang={Uri.EscapeDataString(_lang)}";

        return new HttpRequestMessage
        {
            Method = HttpMethods.Get
        };
    }

    public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
    {
        using (var reader = new StreamReader(stream))
            Result = reader.ReadToEnd();
    }
}
