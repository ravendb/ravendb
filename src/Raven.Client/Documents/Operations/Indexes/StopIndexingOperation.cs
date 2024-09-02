using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// <para>Pauses indexing for the entire database using the StopIndexingOperation.
    /// If you need to stop a single index, use the StopIndexOperation instead.</para>
    /// <para><strong>Note:</strong> Indexing will automatically resume after a server restart or by using the StartIndexingOperation.</para>
    /// </summary>
    public sealed class StopIndexingOperation : IMaintenanceOperation
    {
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StopIndexingCommand();
        }

        internal sealed class StopIndexingCommand : RavenCommand
        {
            private readonly string _type;

            public StopIndexingCommand()
            {
            }

            /// <summary>
            /// For Studio use only
            /// </summary>
            internal StopIndexingCommand(string nodeTag)
            {
                SelectedNodeTag = nodeTag;
            }

            /// <summary>
            /// For Studio use only
            /// </summary>
            internal StopIndexingCommand(string type, string nodeTag)
            {
                _type = type ?? throw new ArgumentNullException(nameof(type));
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/stop";

                if (_type != null)
                    url += $"?type={_type}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
