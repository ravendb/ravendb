using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo : AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo<DatabaseRequestHandler, DocumentsOperationContext>
    {
        private readonly OngoingTasksHandler _ongoingTasksHandler;

        public OngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo([NotNull] OngoingTasksHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
            _ongoingTasksHandler = requestHandler;
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            if (ResourceNameValidator.IsValidResourceName(RequestHandler.Database.Name, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = RequestHandler.GetLongQueryString("key");

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                ClusterTopology clusterTopology;
                PullReplicationDefinition def;
                using (context.OpenReadTransaction())
                {
                    clusterTopology = RequestHandler.ServerStore.GetClusterTopology(context);
                    using (var rawRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.Database.Name))
                    {
                        if (rawRecord == null)
                            throw new DatabaseDoesNotExistException(RequestHandler.Database.Name);

                        def = rawRecord.GetHubPullReplicationById(key);
                    }
                }

                if (def == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var currentHandlers = RequestHandler.Database.ReplicationLoader.OutgoingHandlers.Where(o => o.Destination is ExternalReplication ex && ex.TaskId == key)
                    .Select(x => _ongoingTasksHandler.GetPullReplicationAsHubTaskInfo(clusterTopology, x.Destination as ExternalReplication))
                    .ToList();

                var response = new PullReplicationDefinitionAndCurrentConnections
                {
                    Definition = def,
                    OngoingTasks = currentHandlers
                };

                await _ongoingTasksHandler.WriteResult(context, response.ToJson());
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<PullReplicationDefinitionAndCurrentConnections> command) => RequestHandler.ExecuteRemoteAsync(command);

    }
}
