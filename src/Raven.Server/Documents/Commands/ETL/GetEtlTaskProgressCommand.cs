using System;
using System.Net.Http;
using Raven.Client.Http;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.ETL;

internal sealed class GetEtlTaskProgressCommand : RavenCommand<EtlTaskProgress[]>
{
    private readonly string[] _names;

    public GetEtlTaskProgressCommand(string[] names, string nodeTag)
    {
        _names = names;
        SelectedNodeTag = nodeTag;
    }

    public override bool IsReadRequest => false;

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/etl/progress";

        if (_names is { Length: > 0 })
        {
            for (var i = 0; i < _names.Length; i++)
                url += $"{(i == 0 ? "?" : "&")}name={Uri.EscapeDataString(_names[i])}";
        }

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            return;

        Result = JsonDeserializationServer.EtlTaskProgressResponse(response).Results;
    }

    internal sealed class EtlTaskProgressResponse
    {
        public EtlTaskProgress[] Results { get; set; }
    }
}
