using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Deletes indexing errors using the DeleteIndexErrorsOperation.
    /// This operation clears the errors associated with the index but does not change the index's state from 'Error' to 'Normal.'
    /// 
    /// <para><strong>Note:</strong> This operation is executed only on the server node as defined by the current client configuration.</para>
    /// </summary>
    public sealed class DeleteIndexErrorsOperation : IMaintenanceOperation
    {
        private readonly string[] _indexNames;

        /// <inheritdoc cref="DeleteIndexErrorsOperation" />
        public DeleteIndexErrorsOperation()
        {
        }

        /// <inheritdoc cref="DeleteIndexErrorsOperation" />
        /// <param name="indexNames">An array of index names for which the errors should be deleted.</param>
        public DeleteIndexErrorsOperation(string[] indexNames)
        {
            _indexNames = indexNames;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteIndexErrorsCommand(_indexNames);
        }

        internal sealed class DeleteIndexErrorsCommand : RavenCommand
        {
            private readonly string[] _indexNames;

            public DeleteIndexErrorsCommand(string[] indexNames)
                : this(indexNames, nodeTag: null)
            {
            }

            internal DeleteIndexErrorsCommand(string[] indexNames, string nodeTag)
            {
                _indexNames = indexNames;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/errors";
                if (_indexNames != null && _indexNames.Length > 0)
                {
                    url += "?";
                    foreach (var indexName in _indexNames)
                        url += $"&name={Uri.EscapeDataString(indexName)}";
                }

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }

            public override bool IsReadRequest => false;
        }
    }
}
