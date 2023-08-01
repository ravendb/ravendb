using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public sealed class ResetIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;

        public ResetIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ResetIndexCommand(_indexName);
        }

        internal sealed class ResetIndexCommand : RavenCommand
        {
            private readonly string _indexName;

            public ResetIndexCommand(string indexName)
                : this(indexName, nodeTag: null)
            {
            }

            internal ResetIndexCommand(string indexName, string nodeTag)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Reset
                };
            }
        }
    }
}
