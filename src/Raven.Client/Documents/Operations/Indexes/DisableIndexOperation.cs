using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Disables indexing for a specific index using the DisableIndexOperation. 
    /// Querying a disabled index is still allowed, but results may be stale.
    /// 
    /// <para><strong>Note:</strong> Unlike StopIndex or StopIndexing operations, disabling an index is a persistent operation, meaning the index remains disabled even after a server restart.</para>
    /// </summary>
    public sealed class DisableIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        private readonly bool _clusterWide;

        /// <inheritdoc cref="DisableIndexOperation" />
        /// <param name="indexName">The name of the index to be disabled.</param>
        public DisableIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _clusterWide = false;
        }

        /// <inheritdoc cref="DisableIndexOperation" />
        /// <param name="indexName">The name of the index to be disabled.</param>
        /// <param name="clusterWide">A boolean value indicating whether the index should be disabled cluster-wide across all database-group nodes.</param>
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

        internal sealed class DisableIndexCommand : RavenCommand, IRaftCommand
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
