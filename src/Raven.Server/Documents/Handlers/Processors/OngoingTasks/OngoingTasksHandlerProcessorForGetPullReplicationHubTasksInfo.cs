using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo : AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo<
        DatabaseRequestHandler, DocumentsOperationContext>
    {
        private readonly OngoingTasksHandler _ongoingTasksHandler;

        public OngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo([NotNull] OngoingTasksHandler requestHandler)
            : base(requestHandler)
        {
            _ongoingTasksHandler = requestHandler;
        }

        protected override IEnumerable<OngoingTaskPullReplicationAsHub> GetOngoingTasks(TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology,
            long key)
        {
            return RequestHandler.Database.ReplicationLoader.OutgoingHandlers.Where(o => o.Destination is ExternalReplication ex && ex.TaskId == key)
                .Select(x => _ongoingTasksHandler.GetPullReplicationAsHubTaskInfo(clusterTopology, x.Destination as ExternalReplication))
                .ToList();
        }
    }
}
