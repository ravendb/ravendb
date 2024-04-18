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
        private readonly bool _asSideBySide;

        public ResetIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }
        
        public ResetIndexOperation(string indexName, bool asSideBySide)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _asSideBySide = asSideBySide;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ResetIndexCommand(_indexName, _asSideBySide);
        }

        internal sealed class ResetIndexCommand : RavenCommand
        {
            private readonly string _indexName;
            private readonly bool _asSideBySide;

            public ResetIndexCommand(string indexName, bool asSideBySide = false)
                : this(indexName, asSideBySide: false, nodeTag: null)
            {
                _asSideBySide = asSideBySide;
            }

            internal ResetIndexCommand(string indexName, bool asSideBySide, string nodeTag)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _asSideBySide = asSideBySide;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes?name={Uri.EscapeDataString(_indexName)}&asSideBySide={(_asSideBySide ? "true" : "false")}";

                return new HttpRequestMessage
                {
                    Method = HttpMethods.Reset
                };
            }
        }
    }
}
