using System.Net.Http;
using JetBrains.Annotations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetPulsesLive<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetPulsesLive([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override RavenCommand<object> CreateCommandForNode(string nodeTag) => new GetReplicationPulsesLiveCommand(nodeTag);
    }

    internal class GetReplicationPulsesLiveCommand : RavenCommand
    {
        public GetReplicationPulsesLiveCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/replication/pulses/live";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
    }
}
