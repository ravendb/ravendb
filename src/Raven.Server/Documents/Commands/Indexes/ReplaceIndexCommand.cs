using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

public class ReplaceIndexCommand : RavenCommand
{
    private readonly string _indexName;

    public ReplaceIndexCommand([NotNull] string indexName, string nodeTag)
    {
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/replace?name={Uri.EscapeDataString(_indexName)}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }
}
