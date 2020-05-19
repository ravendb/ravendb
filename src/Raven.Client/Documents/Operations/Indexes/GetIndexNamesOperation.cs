using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexNamesOperation : IMaintenanceOperation<string[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetIndexNamesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<string[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexNamesCommand(_start, _pageSize);
        }

        private class GetIndexNamesCommand : RavenCommand<string[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetIndexNamesCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?start={_start}&pageSize={_pageSize}&namesOnly=true";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetIndexNamesResponse(response).Results;
            }

            public override bool IsReadRequest => true;
        }
    }
}