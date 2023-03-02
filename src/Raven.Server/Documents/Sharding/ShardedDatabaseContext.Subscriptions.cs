using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedSubscriptions SubscriptionsStorage;

    public class ShardedSubscriptions : AbstractSubscriptionStorage<SubscriptionConnectionsStateOrchestrator>
    {
        private readonly ShardedDatabaseContext _context;

        public ShardedSubscriptions(ShardedDatabaseContext context, ServerStore serverStore) : base(serverStore, context.Configuration.Subscriptions.MaxNumberOfConcurrentConnections)
        {
            _context = context;
        }

        public override void Initialize(string name)
        {
            _databaseName = _context.DatabaseName;
            _logger = LoggingSource.Instance.GetLogger<ShardedSubscriptions>(_databaseName);
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
            return _serverStore.WhoseTaskIsIt(databaseRecord.Sharding.Orchestrator.Topology, taskStatus, taskStatus);;
        }

        protected override bool SubscriptionChangeVectorHasChanges(SubscriptionConnectionsStateOrchestrator state, SubscriptionState taskStatus)
        {
            // TODO: egor check if we can drop sharded subscription on CV change (so it reconnects)
            return false;
        }

        public void Update(RawDatabaseRecord databaseRecord)
        {
            HandleDatabaseRecordChange(databaseRecord);
        }
    }
}
