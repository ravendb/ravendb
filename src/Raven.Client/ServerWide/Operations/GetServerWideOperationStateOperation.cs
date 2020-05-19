using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetServerWideOperationStateOperation : IServerOperation<OperationState>
    {
        private readonly long _id;

        public GetServerWideOperationStateOperation(long id)
        {
            _id = id;
        }

        public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideOperationStateCommand(_id);
        }

        internal class GetServerWideOperationStateCommand : RavenCommand<OperationState>
        {
            public override bool IsReadRequest => true;

            private readonly long _id;

            public GetServerWideOperationStateCommand(long id, string nodeTag = null)
            {
                _id = id;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/operations/state?id={_id}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<OperationState>(response);
            }
        }
    }
}
