using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public sealed class StopIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        public StopIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StopIndexCommand(_indexName);
        }

        internal sealed class StopIndexCommand : RavenCommand
        {
            private readonly string _indexName;

            public StopIndexCommand(string indexName)
                : this(indexName, nodeTag: null)
            {
            }

            internal StopIndexCommand(string indexName, string nodeTag)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/stop?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
