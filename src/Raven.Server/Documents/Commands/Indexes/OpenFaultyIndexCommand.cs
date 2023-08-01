using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

public sealed class OpenFaultyIndexCommand : RavenCommand
{
    private readonly string _indexName;

    public OpenFaultyIndexCommand([NotNull] string indexName, string nodeTag)
    {
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/index/open-faulty-index?name={Uri.EscapeDataString(_indexName)}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Post
        };
    }

    public override bool IsReadRequest => false;
}
