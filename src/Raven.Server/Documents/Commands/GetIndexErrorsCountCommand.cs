using System;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands;

internal class GetIndexErrorsCountCommand : RavenCommand<GetIndexErrorsCountCommand.IndexErrorsCount[]>
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

    public class IndexErrorsCount
    {
        public IndexErrorsCount()
        {
        }

        public string Name { get; set; }

        public long NumberOfErrors { get; set; }
    }
}
