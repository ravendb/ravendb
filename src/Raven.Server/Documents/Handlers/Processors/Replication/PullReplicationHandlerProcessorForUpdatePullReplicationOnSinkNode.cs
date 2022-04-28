using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class PullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode : AbstractPullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode<DatabaseRequestHandler>
    {
        public PullReplicationHandlerProcessorForUpdatePullReplicationOnSinkNode([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.Database.Name;

        protected override void FillResponsibleNode(TransactionOperationContext context, DynamicJsonValue responseJson, PullReplicationAsSink pullReplication)
        {
            var databaseName = GetDatabaseName();

            using (context.OpenReadTransaction())
            {
                var topology = RequestHandler.ServerStore.Cluster.ReadDatabaseTopology(context, databaseName);
                responseJson[nameof(OngoingTask.ResponsibleNode)] = RequestHandler.ServerStore.WhoseTaskIsIt(topology, pullReplication, null);
            }
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
        }
    }
}
