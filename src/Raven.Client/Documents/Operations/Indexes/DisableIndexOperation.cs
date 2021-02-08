using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public class DisableIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        private readonly bool _clusterWide;

        public DisableIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _clusterWide = false;
        }

        public DisableIndexOperation(string indexName, bool clusterWide)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _clusterWide = clusterWide;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DisableIndexCommand(_indexName, _clusterWide);
        }

        private class DisableIndexCommand : RavenCommand, IRaftCommand
        {
            private readonly string _indexName;
            private readonly bool _clusterWide;

            public DisableIndexCommand(string indexName, bool clusterWide)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _clusterWide = clusterWide;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/disable?name={Uri.EscapeDataString(_indexName)}&clusterWide={_clusterWide}";

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
