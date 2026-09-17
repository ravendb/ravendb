using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

internal sealed class GetIndexesProgressCommand : RavenCommand<IndexProgress[]>
{
    private readonly string[] _names;
    private readonly bool _exact;

    public GetIndexesProgressCommand(string nodeTag, string[] names = null, bool exact = false)
    {
        SelectedNodeTag = nodeTag;
        _names = names;
        _exact = exact;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/progress";

        var separator = '?';

        if (_names is { Length: > 0 })
        {
            for (var i = 0; i < _names.Length; i++)
            {
                url += $"{separator}name={Uri.EscapeDataString(_names[i])}";
                separator = '&';
            }
        }

        if (_exact)
        {
            url += $"{separator}exact=true";
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

        Result = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexesProgress>(response).Results;
    }

    public override bool IsReadRequest => true;
}
