using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetOperationStateCommand : RavenCommand<OperationState>
    {
        private readonly DocumentConventions _conventions;
        private readonly long _id;

        public GetOperationStateCommand(DocumentConventions conventions, long id)
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

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            
            Result = (OperationState)_conventions.DeserializeEntityFromBlittable(typeof(OperationState), response);
        }

        public override bool IsReadRequest => true;
    }
}