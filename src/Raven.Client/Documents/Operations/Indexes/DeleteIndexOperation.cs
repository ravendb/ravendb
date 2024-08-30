using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Removes an index from the database using the DeleteIndexOperation.
    /// 
    /// <para><strong>Note:</strong> The index will be deleted from all database-group nodes.</para>
    /// </summary>
    public sealed class DeleteIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        /// <inheritdoc cref="DeleteIndexOperation" />
        /// <param name="indexName">The name of the index to be deleted.</param>
        public DeleteIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteIndexCommand(_indexName);
        }

        private sealed class DeleteIndexCommand : RavenCommand, IRaftCommand
        {
            private readonly string _indexName;

            public DeleteIndexCommand(string indexName)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Delete
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
