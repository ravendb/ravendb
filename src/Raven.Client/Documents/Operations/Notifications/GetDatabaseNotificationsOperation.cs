using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Notifications;

public sealed class GetDatabaseNotificationsOperation : IMaintenanceOperation<BlittableJsonReaderObject>
{
    private readonly string _nodeTag;
    
    public GetDatabaseNotificationsOperation()
    {
    }
    
    internal GetDatabaseNotificationsOperation(string nodeTag = null)
    {
        _nodeTag = nodeTag;
    }
    
    public RavenCommand<BlittableJsonReaderObject> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetDatabaseNotificationsCommand(_nodeTag);
    }

    internal sealed class GetDatabaseNotificationsCommand : RavenCommand<BlittableJsonReaderObject>
    {
        public GetDatabaseNotificationsCommand(string nodeTag)
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
}
