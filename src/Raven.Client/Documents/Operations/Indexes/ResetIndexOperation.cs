using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Rebuilds an index using the ResetIndexOperation. This operation removes all existing indexed data and re-indexes all items matched by the index definition.
    /// 
    /// <para><strong>Indexes scope:</strong> Both static and auto indexes can be reset.</para>
    /// 
    /// <para><strong>Nodes scope:</strong></para>
    /// <list type="bullet">
    /// <item>
    /// <description>When resetting an index from the client, the index is reset only on the preferred node, not across all database-group nodes.</description>
    /// </item>
    /// <item>
    /// <description>When resetting an index from the Studio indexes list view, the index is reset on the local node where the browser is opened, even if it is not the preferred node.</description>
    /// </item>
    /// </list>
    /// <para>If the index is disabled or paused, resetting will return it to the normal running state on the local node where the action was performed.</para>
    /// </summary>
    public sealed class ResetIndexOperation : IMaintenanceOperation
    {
        private readonly string _indexName;
        private readonly IndexResetMode? _indexResetMode;

        /// <inheritdoc cref="ResetIndexOperation" />
        /// <param name="indexName">The name of the index to be reset.</param>
        public ResetIndexOperation(string indexName)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        }

        /// <inheritdoc cref="ResetIndexOperation" />
        /// <param name="indexName">The name of the index to be reset.</param>
        /// <param name="indexResetMode">The mode to use when resetting the index. Valid values are InPlace and SideBySide.</param>
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
