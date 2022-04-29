using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes
{
    internal abstract class AbstractAdminIndexHandlerProcessorForDump<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractAdminIndexHandlerProcessorForDump([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand CreateCommandForNode(string nodeTag)
        {
            var (name, path) = GetParameters();
            return new IndexesDumpCommand(name, path, nodeTag);
        }

        protected (string Name, string Path) GetParameters()
        {
            return (RequestHandler.GetStringQueryString("name"), RequestHandler.GetStringQueryString("path"));
        }

        internal class IndexesDumpCommand : RavenCommand
        {
            private readonly string _name;
            private readonly string _path;

            internal IndexesDumpCommand(string name, string path)
            {
                _name = name;
                _path = path;
            }

            internal IndexesDumpCommand(string name, string path, string nodeTag) : this(name, path)
            {
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/indexes/dump?name={_name}&path={_path}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }
        }
    }
}
