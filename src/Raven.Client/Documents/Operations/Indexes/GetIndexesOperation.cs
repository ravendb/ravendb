using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexesOperation : IMaintenanceOperation<IndexDefinition[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetIndexesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<IndexDefinition[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
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

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetIndexesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}