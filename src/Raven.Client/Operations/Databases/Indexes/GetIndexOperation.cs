using System;
using System.Net.Http;
using Raven.Client.Commands;
using Raven.Client.Document;
using Raven.Client.Http;
using Raven.Client.Indexing;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases.Indexes
{
    public class GetIndexOperation : IAdminOperation<IndexDefinition>
    {
        private readonly string _indexName;

        public GetIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
        }

        public RavenCommand<IndexDefinition> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetIndexCommand(_indexName);
        }

        private class GetIndexCommand : RavenCommand<IndexDefinition>
        {
            private readonly string _indexName;

            public GetIndexCommand(string indexName)
            {
                if (indexName == null)
                    throw new ArgumentNullException(nameof(indexName));

                _indexName = indexName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeUriString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetIndexesResponse(response).Results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}