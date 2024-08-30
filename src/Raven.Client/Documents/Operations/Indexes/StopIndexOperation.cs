using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// <para>Pauses a single index in the database using the StopIndexOperation.
    /// A paused index performs no indexing on the node it is paused for, but continues indexing new data is indexed by the index on database-group nodes where the index is not paused.
    /// Although a paused index can still be queried, results may be stale when querying the node where the index is paused.</para>
    /// <para><strong>Notes:</strong> </para>
    /// <list type="bullet">
    /// <item>
    /// <description>The index will be paused only on the preferred node, not across all database-group nodes.</description>
    /// </item>
    /// <item>
    /// <description>To pause indexing for all indexes in the database, use the StopIndexingOperation.</description>
    /// </item>
    /// </list>
    /// </summary>
    public sealed class StopIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        /// <inheritdoc cref="StopIndexOperation" />
        /// <param name="indexName">The name of the index to be paused.</param>
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
