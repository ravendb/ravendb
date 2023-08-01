using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

public sealed class GetIndexStalenessCommand : RavenCommand<GetIndexStalenessCommand.IndexStaleness>
{
    private readonly string _indexName;

    public GetIndexStalenessCommand(string indexName, string nodeTag)
    {
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/staleness?name={Uri.EscapeDataString(_indexName)}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<IndexStaleness>(response);
    }

    public sealed class IndexStaleness
    {
        public bool IsStale { get; set; }

        public List<string> StalenessReasons { get; set; }
    }
}
