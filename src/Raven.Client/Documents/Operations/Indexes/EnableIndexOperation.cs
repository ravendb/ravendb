using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class EnableIndexOperation : IAdminOperation
    {
        private readonly string _indexName;

        public EnableIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new EnableIndexCommand(_indexName);
        }

        private class EnableIndexCommand : RavenCommand
        {
            private readonly string _indexName;

            public EnableIndexCommand(string indexName)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/enable?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}