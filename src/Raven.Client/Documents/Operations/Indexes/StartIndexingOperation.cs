using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Resumes indexing for the entire database using the StartIndexingOperation.
    /// This operation is typically used after indexing has been paused or disabled.
    /// </summary>
    public sealed class StartIndexingOperation : IMaintenanceOperation
    {
        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartIndexingCommand();
        }

        internal sealed class StartIndexingCommand : RavenCommand
        {
            private readonly string _type;

            public StartIndexingCommand()
            {
            }

            /// <summary>
            /// For Studio use only
            /// </summary>
            internal StartIndexingCommand(string nodeTag)
            {
                SelectedNodeTag = nodeTag;
            }

            /// <summary>
            /// For Studio use only
            /// </summary>
            internal StartIndexingCommand(string type, string nodeTag)
            {
                _type = type ?? throw new ArgumentNullException(nameof(type));
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/start";

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
