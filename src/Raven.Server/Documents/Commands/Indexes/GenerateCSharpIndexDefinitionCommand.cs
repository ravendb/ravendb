using System;
using System.IO;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

public sealed class GenerateCSharpIndexDefinitionCommand : RavenCommand<string>
{
    private readonly string _indexName;

    public override bool IsReadRequest => true;

    public GenerateCSharpIndexDefinitionCommand([NotNull] string indexName, string nodeTag)
    {
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        SelectedNodeTag = nodeTag;
        ResponseType = RavenCommandResponseType.Raw;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/c-sharp-index-definition?name={Uri.EscapeDataString(_indexName)}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponseRaw(HttpResponseMessage response, Stream stream, JsonOperationContext context)
    {
        using (var reader = new StreamReader(stream))
            Result = reader.ReadToEnd();
    }
}
