using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Transformers
{
    public class GetTransformerNamesOperation : IAdminOperation<string[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetTransformerNamesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<string[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetTransformerNamesCommand(_start, _pageSize);
        }

        private class GetTransformerNamesCommand : RavenCommand<string[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetTransformerNamesCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?start={_start}&pageSize={_pageSize}&namesOnly=true";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetTransformerNamesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}