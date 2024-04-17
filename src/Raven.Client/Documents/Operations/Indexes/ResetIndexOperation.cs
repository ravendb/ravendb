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
        private readonly bool _isSideBySide;

        public ResetIndexOperation(string indexName, bool isSideBySide = false)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _isSideBySide = isSideBySide;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ResetIndexCommand(_indexName, _isSideBySide);
        }

        internal sealed class ResetIndexCommand : RavenCommand
        {
            private readonly string _indexName;
            private readonly bool _isSideBySide;

            public ResetIndexCommand(string indexName, bool isSideBySide = false)
                : this(indexName, isSideBySide: false, nodeTag: null)
            {
                _isSideBySide = isSideBySide;
            }

            internal ResetIndexCommand(string indexName, bool isSideBySide, string nodeTag)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _isSideBySide = isSideBySide;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}&isSideBySide={(_isSideBySide ? "true" : "false")}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Reset
                };
            }
        }
    }
}
