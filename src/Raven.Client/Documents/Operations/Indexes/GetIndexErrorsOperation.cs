using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class GetIndexErrorsOperation : IAdminOperation<IndexErrors[]>
    {
        private readonly string[] _indexNames;

        public GetIndexErrorsOperation()
        {
        }

        public GetIndexErrorsOperation(string[] indexNames)
        {
            _indexNames = indexNames;
        }

        public RavenCommand<IndexErrors[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetIndexErrorsCommand(_indexNames);
        }

        public class GetIndexErrorsCommand : RavenCommand<IndexErrors[]>
        {
            private readonly string[] _indexNames;

            public GetIndexErrorsCommand(string[] indexNames)
            {
                _indexNames = indexNames;
                ResponseType = RavenCommandResponseType.Array;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/errors";
                if (_indexNames != null && _indexNames.Length > 0)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                        url += $"&name={indexName}";
                }

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                ThrowInvalidResponse();
            }

            public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var indexErrors = new IndexErrors[response.Length];
                for (int i = 0; i < response.Length; i++)
                {
                    indexErrors[i] = JsonDeserializationClient.IndexErrors((BlittableJsonReaderObject)response[i]);
                }

                Result = indexErrors;
            }

            public override bool IsReadRequest => true;
        }
    }
}