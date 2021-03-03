using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class EnableIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        private readonly bool _clusterWide;

        public EnableIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _clusterWide = false;
        }

        public EnableIndexOperation(string indexName, bool clusterWide)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _clusterWide = clusterWide;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new EnableIndexCommand(_indexName, _clusterWide);
        }

        private class EnableIndexCommand : RavenCommand, IRaftCommand
        {
            private readonly string _indexName;
            private readonly bool _clusterWide;

            public EnableIndexCommand(string indexName, bool clusterWide)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _clusterWide = clusterWide;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/enable?name={Uri.EscapeDataString(_indexName)}&clusterWide={_clusterWide}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
