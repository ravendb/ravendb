using System;
using System.IO;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands;

public class GenerateClassFromDocumentCommand : RavenCommand<string>
{
    private readonly string _id;
    private readonly string _lang;

    public GenerateClassFromDocumentCommand(string id, string lang)
    {
        _id = id ?? throw new ArgumentException(nameof(id));
        _lang = lang;
        ResponseType = RavenCommandResponseType.Raw;
    }

    public override bool IsReadRequest => true;
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/docs/class?id={Uri.EscapeDataString(_id)}&lang={Uri.EscapeDataString(_lang)}";

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
