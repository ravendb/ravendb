using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
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

        public RavenCommand<IndexDefinition> GetCommand(DocumentConvention conventions)
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

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.GetIndexesResponse(response).Results[0];
            }

            public override bool IsReadRequest => true;
        }
    }
}