using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Server.Operations.ApiKeys
{
    public class GetApiKeysOperation : IServerOperation<NamedApiKeyDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetApiKeysOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<NamedApiKeyDefinition[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetApiKeysCommand(_start, _pageSize);
        }

        private class GetApiKeysCommand : RavenCommand<NamedApiKeyDefinition[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetApiKeysCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/api-keys?start={_start}&pageSize={_pageSize}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetApiKeysResponse(response).Results;
            }
        }
    }
}