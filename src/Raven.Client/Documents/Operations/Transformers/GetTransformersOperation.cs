using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Transformers;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class GetTransformersOperation : IAdminOperation<TransformerDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetTransformersOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<TransformerDefinition[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetTransformersCommand(_start, _pageSize);
        }

        private class GetTransformersCommand : RavenCommand<TransformerDefinition[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetTransformersCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetTransformersResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}