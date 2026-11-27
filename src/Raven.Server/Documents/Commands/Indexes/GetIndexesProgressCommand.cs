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
    private readonly string[] _excludedNames;

    public GetIndexesProgressCommand(string nodeTag, string[] names = null, bool exact = false, string[] excludedNames = null)
    {
        SelectedNodeTag = nodeTag;
        _names = names;
        _exact = exact;
        _excludedNames = excludedNames;
    }

    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/progress";

        char separator = '?';

        AppendNames(ref url, ref separator, "name", _names);
        AppendNames(ref url, ref separator, "excludeName", _excludedNames);

        if (_exact)
        {
            url += $"{separator}exact=true";
        }

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }

    private static void AppendNames(ref string url, ref char separator, string parameterName, string[] names)
    {
        if (names is not { Length: > 0 })
            return;

        foreach (string name in names)
        {
            url += $"{separator}{parameterName}={Uri.EscapeDataString(name)}";
            separator = '&';
        }
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
