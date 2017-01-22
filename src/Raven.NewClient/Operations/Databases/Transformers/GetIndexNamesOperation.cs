using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Operations;
using Sparrow.Json;

namespace Raven.NewClient.Client.Operations.Databases.Transformers
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

        public RavenCommand<string[]> GetCommand(DocumentConvention conventions, JsonOperationContext context)
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

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/transformers?start={_start}&pageSize={_pageSize}&namesOnly=true";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetTransformerNamesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}