using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    public sealed class ResetIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;
        private readonly IndexResetMode? _indexResetMode;

        public ResetIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }
        
        public ResetIndexOperation(string indexName, IndexResetMode indexResetMode)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _indexResetMode = indexResetMode;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ResetIndexCommand(_indexName, _indexResetMode);
        }

        internal sealed class ResetIndexCommand : RavenCommand
        {
            private readonly string _indexName;
            private readonly IndexResetMode? _indexResetMode;

            public ResetIndexCommand(string indexName, IndexResetMode? indexResetMode)
                : this(indexName, indexResetMode, nodeTag: null)
            {
                _indexResetMode = indexResetMode;
            }

            internal ResetIndexCommand(string indexName, IndexResetMode? indexResetMode, string nodeTag)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _indexResetMode = indexResetMode;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}";

                if (_indexResetMode is not null)
                    url += $"&mode={_indexResetMode.ToString()}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Reset
                };
            }
        }
    }
}
