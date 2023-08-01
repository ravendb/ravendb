using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

internal sealed class GetIndexErrorsCountCommand : RavenCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]>
{
    private readonly string[] _indexNames;

    internal GetIndexErrorsCountCommand(string[] indexNames, string nodeTag)
    {
        _indexNames = indexNames;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/studio/indexes/errors-count";
        if (_indexNames != null && _indexNames.Length > 0)
        {
            url += "?";
            foreach (var indexName in _indexNames)
                url += $"&name={Uri.EscapeDataString(indexName)}";
        }

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null ||
            response.TryGet("Results", out BlittableJsonReaderArray results) == false)
        {
            ThrowInvalidResponse();
            return; // never hit
        }

        var indexErrors = new IndexErrorsCount[results.Length];
        for (int i = 0; i < results.Length; i++)
        {
            indexErrors[i] = JsonDeserializationServer.IndexErrorsCount((BlittableJsonReaderObject)results[i]);
        }

        Result = indexErrors;
    }

    public sealed class IndexErrorsCount
    {
        public IndexErrorsCount()
        {
            Errors = Array.Empty<IndexingErrorCount>();
        }

        public string Name { get; set; }

        public IndexingErrorCount[] Errors { get; set; }
    }

    public sealed class IndexingErrorCount
    {
        public string Action { get; set; }

        public long NumberOfErrors { get; set; }
    }
}
