using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedSubscriptionsStorage SubscriptionsStorage;

    public class ShardedSubscriptionsStorage : AbstractSubscriptionStorage<SubscriptionConnectionsStateOrchestrator>
    {
        private readonly ShardedDatabaseContext _context;

        public ShardedSubscriptionsStorage(ShardedDatabaseContext context, ServerStore serverStore) : base(serverStore, context.Configuration.Subscriptions.MaxNumberOfConcurrentConnections)
        {
            _context = context;
            _databaseName = _context.DatabaseName;
            _logger = LoggingSource.Instance.GetLogger<ShardedSubscriptionsStorage>(_databaseName);
        }

        protected override void DropSubscriptionConnections(SubscriptionConnectionsStateOrchestrator state, SubscriptionException ex)
        {
            state.DisposeWorkers();
            foreach (var subscriptionConnection in state.GetConnections())
            {
                state.DropSingleConnection(subscriptionConnection, ex);
            }
        }

        protected override void SetConnectionException(SubscriptionConnectionsStateOrchestrator state, SubscriptionException ex)
        {
            foreach (var connection in state.GetConnections())
            {
                // this is just to set appropriate exception, the connections will be dropped on state dispose
                connection.ConnectionException = ex;
            }
        }

        protected override string GetSubscriptionResponsibleNode(DatabaseRecord databaseRecord, SubscriptionState taskStatus)
        {
            return _serverStore.WhoseTaskIsIt(databaseRecord.Sharding.Orchestrator.Topology, taskStatus, taskStatus);
        }

        protected override bool SubscriptionChangeVectorHasChanges(SubscriptionConnectionsStateOrchestrator state, SubscriptionState taskStatus)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "RavenDB-18223 Add ability to set CV by admin in sharded subscription.");
            return false;
        }

        public override (OngoingTaskConnectionStatus ConnectionStatus, string ResponsibleNodeTag) GetSubscriptionConnectionStatusAndResponsibleNode(
            long subscriptionId,
            SubscriptionState state,
            [NotNull] DatabaseRecord databaseRecord)
        {
            if (databaseRecord == null) 
                throw new ArgumentNullException(nameof(databaseRecord));

            return GetSubscriptionConnectionStatusAndResponsibleNode(subscriptionId, state, databaseRecord.Sharding.Orchestrator.Topology);
        }

        public void Update(RawDatabaseRecord databaseRecord)
        {
            HandleDatabaseRecordChange(databaseRecord);
        }
    }
}
