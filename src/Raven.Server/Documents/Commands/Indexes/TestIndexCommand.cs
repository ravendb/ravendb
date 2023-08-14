using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents.Indexes.Test;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Indexes;

internal sealed class TestIndexCommand : RavenCommand<BlittableJsonReaderObject>
{
    private readonly TestIndexParameters _parameters;
    private readonly DocumentConventions _documentConventions;
    
    public override bool IsReadRequest => true;
    
    public TestIndexCommand(DocumentConventions documentConventions, string nodeTag, TestIndexParameters parameters)
    {
        _documentConventions = documentConventions;
        SelectedNodeTag = nodeTag;
        _parameters = parameters;
    }
    
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/indexes/test";
        
        var parametersJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx);
        
        return new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            Content = new BlittableJsonContent(async stream =>
            {
                await ctx.WriteAsync(stream, parametersJson);
            }, _documentConventions)
        };
    }
    
    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
        {
            ThrowInvalidResponse();
            return; // never hit
        }

        Result = response.Clone(context);
    }
}
