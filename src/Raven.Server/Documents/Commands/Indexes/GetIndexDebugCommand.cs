using System;
using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

public class GetIndexDebugCommand : RavenCommand
{
    private readonly string _indexName;
    private readonly string _op;
    private readonly string[] _docIds;
    private readonly string _startsWith;
    private readonly int? _start;
    private readonly int? _pageSize;

    public GetIndexDebugCommand([NotNull] string indexName, [NotNull] string op, string[] docIds, string startsWith, int? start, int? pageSize, string nodeTag)
    {
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        _op = op ?? throw new ArgumentNullException(nameof(op));
        _docIds = docIds;
        _startsWith = startsWith;
        _start = start;
        _pageSize = pageSize;
        SelectedNodeTag = nodeTag;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/debug?name={Uri.EscapeDataString(_indexName)}&op={Uri.EscapeDataString(_op)}";

        if (_docIds is { Length: > 0 })
        {
            foreach (string docId in _docIds)
                url += $"&docId={Uri.EscapeDataString(docId)}";
        }

        if (_startsWith != null)
            url += $"&startsWith={Uri.EscapeDataString(_startsWith)}";

        if (_start != null && _start != 0)
            url += $"&start={_start}";

        if (_pageSize != null && _pageSize != int.MaxValue)
            url += $"&pageSize={_pageSize}";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public static GetIndexDebugCommand GetMapReduceTree(string indexName, string[] docIds, string nodeTag)
    {
        return new GetIndexDebugCommand(indexName, "map-reduce-tree", docIds, startsWith: null, start: null, pageSize: null, nodeTag);
    }

    public static GetIndexDebugCommand GetSourceDocIds(string indexName, string startsWith, int start, int pageSize, string nodeTag)
    {
        return new GetIndexDebugCommand(indexName, "source-doc-ids", docIds: null, startsWith: startsWith, start: start, pageSize: pageSize, nodeTag);
    }

    public static GetIndexDebugCommand GetEntriesFields(string indexName, string nodeTag)
    {
        return new GetIndexDebugCommand(indexName, "entries-fields", docIds: null, startsWith: null, start: null, pageSize: null, nodeTag);
    }
}
