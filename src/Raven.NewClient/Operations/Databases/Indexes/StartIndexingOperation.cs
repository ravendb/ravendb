using System;
using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Indexes
{
    public class StartIndexingOperation : IAdminOperation
    {
        private readonly string _indexName;

        public StartIndexingOperation()
        {
        }

        public StartIndexingOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
        }

        public RavenCommand<object> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new StartIndexingCommand(_indexName);
        }

        private class StartIndexingCommand : RavenCommand<object>
        {
            private readonly string _indexName;

            public StartIndexingCommand(string indexName)
            {
                _indexName = indexName;
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/start";

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