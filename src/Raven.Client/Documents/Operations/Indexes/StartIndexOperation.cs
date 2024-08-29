using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// <para>Resumes indexing for a specific index using the StartIndexOperation. 
    /// This operation is typically used after an index has been paused with StopIndexOperation.</para>
    /// <para><strong>Note:</strong> The index is resumed only on the preferred node, not across all database-group nodes.</para>
    /// </summary>
    public sealed class StartIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        /// <inheritdoc cref="StartIndexOperation" />
        /// <param name="indexName">The name of the index to resume indexing for.</param>
        public StartIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartIndexCommand(_indexName);
        }

        internal sealed class StartIndexCommand : RavenCommand
        {
            private readonly string _indexName;

            public StartIndexCommand(string indexName)
                : this(indexName, nodeTag: null)
            {
            }

            internal StartIndexCommand(string indexName, string nodeTag)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/start?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
