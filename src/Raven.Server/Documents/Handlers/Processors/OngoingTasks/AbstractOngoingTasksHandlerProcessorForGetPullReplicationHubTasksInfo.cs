using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<PullReplicationDefinitionAndCurrentConnections, TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        protected override RavenCommand<PullReplicationDefinitionAndCurrentConnections> CreateCommandForNode(string nodeTag)
        {
            var key = RequestHandler.GetLongQueryString("key");
            return new GetPullReplicationTasksInfoOperation.GetPullReplicationTasksInfoCommand(key, nodeTag);
        }
    }
}
