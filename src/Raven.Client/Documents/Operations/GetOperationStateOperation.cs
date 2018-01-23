using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetOperationStateOperation : IMaintenanceOperation<OperationState>
    {
        private readonly long _id;

        public GetOperationStateOperation(long id)
        {
            _id = id;
        }

        public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetOperationStateCommand(conventions, _id);
        }

        internal class GetOperationStateCommand : RavenCommand<OperationState>
        {
            public override bool IsReadRequest => true;

            private readonly DocumentConventions _conventions;
            private readonly long _id;

            public GetOperationStateCommand(DocumentConventions conventions, long id)
            {
                _conventions = conventions;
                _id = id;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/operations/state?id={_id}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = (OperationState)_conventions.DeserializeEntityFromBlittable(typeof(OperationState), response);
            }
        }
    }
}
