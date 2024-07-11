using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedSubscriptionsStorage SubscriptionsStorage;

    public sealed class ShardedSubscriptionsStorage : AbstractSubscriptionStorage<SubscriptionConnectionsStateOrchestrator>
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

        protected override string GetNodeFromState(SubscriptionState taskStatus) => taskStatus.NodeTag;

        protected override DatabaseTopology GetTopology(ClusterOperationContext context) => _serverStore.Cluster.ReadShardingConfiguration(context, _databaseName)?.Orchestrator.Topology;

        protected override bool SubscriptionChangeVectorHasChanges(SubscriptionConnectionsStateOrchestrator state, SubscriptionState taskStatus)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "RavenDB-18223 Add ability to set CV by admin in sharded subscription.");
            return false;
        }

        public void Update()
        {
            HandleDatabaseRecordChange();
        }

        public override bool DropSingleSubscriptionConnection(long subscriptionId, string workerId, SubscriptionException ex)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Egor, DevelopmentHelper.Severity.Normal, "RavenDB-18568: need to handle workerId for concurrent subscription");
            // for sharded database there is no concurrent subscription
            return DropSubscriptionConnections(subscriptionId, ex);
        }

        public override bool DisableSubscriptionTasks => false;

        public override ArchivedDataProcessingBehavior GetDefaultArchivedDataProcessingBehavior()
        {
            return _context.Configuration.Subscriptions.ArchivedDataProcessingBehavior;
        }
    }
}
