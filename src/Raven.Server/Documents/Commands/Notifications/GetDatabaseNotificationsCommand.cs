using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Notifications;

public sealed class GetDatabaseNotificationsCommand : RavenCommand<BlittableJsonReaderObject>
{
    public GetDatabaseNotificationsCommand(string nodeTag = null)
    {
        SelectedNodeTag = nodeTag;
    }
        
    public override bool IsReadRequest => true;
    
    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
    {
        url = $"{node.Url}/databases/{node.Database}/notifications";

        return new HttpRequestMessage
        {
            Method = HttpMethod.Get
        };
    }
        
    public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
    {
        if (response == null)
            ThrowInvalidResponse();

        Result = response;
    }
}
