using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Indexing;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
{
    public class GetIndexesOperation : IAdminOperation<IndexDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetIndexesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<IndexDefinition[]> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetIndexesCommand(_start, _pageSize);
        }

        private class GetIndexesCommand : RavenCommand<IndexDefinition[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetIndexesCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetIndexesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}