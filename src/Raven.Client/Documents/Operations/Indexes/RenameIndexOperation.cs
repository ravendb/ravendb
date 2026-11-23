using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Renames a static index. The index data stays as it is: nothing is re-indexed.
    ///
    /// <para><strong>Note:</strong> The operation returns when the cluster accepted the rename. The nodes of the database group then rename the index
    /// one at a time. Until all nodes are done, a query by the new name on a node that did not rename yet, or by the old name on a node that did,
    /// gets an error, and the client tries another node.</para>
    /// </summary>
    public sealed class RenameIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;
        private readonly string _newIndexName;

        /// <inheritdoc cref="RenameIndexOperation" />
        /// <param name="indexName">The current name of the index.</param>
        /// <param name="newIndexName">The new name of the index.</param>
        public RenameIndexOperation(string indexName, string newIndexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _newIndexName = newIndexName ?? throw new ArgumentNullException(nameof(newIndexName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RenameIndexCommand(_indexName, _newIndexName);
        }

        private sealed class RenameIndexCommand : RavenCommand, IRaftCommand
        {
            private readonly string _indexName;
            private readonly string _newIndexName;

            public RenameIndexCommand(string indexName, string newIndexName)
            {
                _indexName = indexName;
                _newIndexName = newIndexName;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/rename?name={Uri.EscapeDataString(_indexName)}&newName={Uri.EscapeDataString(_newIndexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
