using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<PullReplicationDefinitionAndCurrentConnections, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override RavenCommand<PullReplicationDefinitionAndCurrentConnections> CreateCommandForNode(string nodeTag)
        {
            var key = RequestHandler.GetLongQueryString("key");
            return new GetPullReplicationTasksInfoOperation.GetPullReplicationTasksInfoCommand(key, nodeTag);
        }
    }
}
