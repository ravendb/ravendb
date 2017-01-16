using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class StopIndexingOperation : IAdminOperation
    {
        private readonly string _indexName;

        public StopIndexingOperation()
        {
        }

        public StopIndexingOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new StopIndexingCommand(_indexName);
        }

        private class StopIndexingCommand : RavenCommand<object>
        {
            private readonly string _indexName;

            public StopIndexingCommand(string indexName)
            {
                _indexName = indexName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/stop";

                if (string.IsNullOrWhiteSpace(_indexName) == false)
                    url += $"?name={Uri.EscapeUriString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
            }

            public override bool IsReadRequest => false;
        }
    }
}