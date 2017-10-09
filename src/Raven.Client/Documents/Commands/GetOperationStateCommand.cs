using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class GetOperationStateOperation : IServerOperation<OperationState>
    {
        private readonly long _id;
        private readonly bool _isServerStoreOperation;

        public GetOperationStateOperation(long id, bool isServerStoreOperation)
        {
            _id = id;
            _isServerStoreOperation = isServerStoreOperation;
        }

        public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetOperationStateCommand(DocumentConventions.Default, _id, _isServerStoreOperation);
        }
    }

    public class GetOperationStateCommand : RavenCommand<OperationState>
    {
        public override bool IsReadRequest => true;

        private readonly DocumentConventions _conventions;
        private readonly long _id;
        private readonly bool _isServerStoreOperation;

        public GetOperationStateCommand(DocumentConventions conventions, long id, bool isServerStoreOperation = false)
        {
            _conventions = conventions;
            _id = id;
            _isServerStoreOperation = isServerStoreOperation;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = _isServerStoreOperation == false ? 
                $"{node.Url}/databases/{node.Database}/operations/state?id={_id}" : 
                $"{node.Url}/operations/state?id={_id}";

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
    }
}
