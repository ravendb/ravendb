using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors;

internal abstract class AbstractStudioDatabaseTasksHandlerProcessorForRestartDatabase<TRequestHandler, TOperationContext> : AbstractHandlerProxyNoContentProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStudioDatabaseTasksHandlerProcessorForRestartDatabase([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag) => new RestartDatabaseCommand(nodeTag);

    private class RestartDatabaseCommand : RavenCommand
    {
        public RestartDatabaseCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/studio-tasks/restart";
            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}
