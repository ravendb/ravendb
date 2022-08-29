using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetPerformanceLive<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetPerformanceLive([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<object> CreateCommandForNode(string nodeTag) => new GetReplicationPerformanceLiveCommand(nodeTag);
    }

    internal class GetReplicationPerformanceLiveCommand : RavenCommand
    {
        public GetReplicationPerformanceLiveCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/performance/live";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    }
}
