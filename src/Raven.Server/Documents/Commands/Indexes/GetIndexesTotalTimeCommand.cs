using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

internal class GetIndexesTotalTimeCommand : RavenCommand<GetIndexesTotalTimeCommand.IndexTotalTime[]>
{
    private readonly string[] _indexNames;

    public GetIndexesTotalTimeCommand(string[] indexNames, string nodeTag)
    {
        _indexNames = indexNames;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => true;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/total-time";

        if (_indexNames is { Length: > 0 })
        {
            url += "?";
            foreach (var indexName in _indexNames)
            {
                url += $"&name={Uri.EscapeDataString(indexName)}";
            }
        }

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            ThrowInvalidResponse();
            return; // never hit
        }

        Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexesTotalTime>(response).Results;
    }

    internal class IndexesTotalTime
    {
        public IndexTotalTime[] Results { get; set; }
    }

    internal class IndexTotalTime
    {
        public string Name { get; set; }

        public TimeSpan TotalIndexingTime { get; set; }

        public TimeSpan LagTime { get; set; }
    }
}
