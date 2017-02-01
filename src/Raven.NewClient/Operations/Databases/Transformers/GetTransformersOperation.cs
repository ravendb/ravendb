using System.Net.Http;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Raven.NewClient.Operations;
using Sparrow.Json;

namespace Raven.NewClient.Client.Operations.Databases.Transformers
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

        public RavenCommand<TransformerDefinition[]> GetCommand(DocumentConvention conventions, JsonOperationContext context)
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

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
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