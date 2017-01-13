using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Commands
{
    public class GetOperationStateCommand : RavenCommand<OperationState>
    {
        private readonly DocumentConvention _conventions;
        private readonly long _id;

        public GetOperationStateCommand(DocumentConvention conventions, long id)
        {
            _conventions = conventions;
            _id = id;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/operations/state?id={_id}";

           return new HttpRequestMessage
           {
               Method = HttpMethod.Get
           };
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
                return;

            
            Result = (OperationState)_conventions.DeserializeEntityFromBlittable(typeof(OperationState), response);
        }

        public override bool IsReadRequest => true;
    }
}