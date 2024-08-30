using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Enables an index using the EnableIndexOperation. Once enabled, the index will begin indexing new data.
    /// 
    /// <para><strong>Scope:</strong> The index can be enabled in two ways:</para>
    /// <list type="bullet">
    /// <item>
    /// <description>On a single node.</description>
    /// </item>
    /// <item>
    /// <description>Cluster-wide, across all database-group nodes.</description>
    /// </item>
    /// </list>
    /// </summary>
    public sealed class EnableIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        private readonly bool _clusterWide;

        /// <inheritdoc cref="EnableIndexOperation" />
        /// <param name="indexName">The name of the index to be enabled.</param>
        public EnableIndexOperation(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _indexName = indexName;
            _clusterWide = false;
        }
        /// <inheritdoc cref="EnableIndexOperation" />
        /// <param name="indexName">The name of the index to be enabled.</param>
        /// <param name="clusterWide">A boolean value indicating whether the index should be enabled cluster-wide across all database-group nodes.</param>
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

        internal sealed class EnableIndexCommand : RavenCommand, IRaftCommand
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
