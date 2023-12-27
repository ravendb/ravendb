using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;

namespace Raven.Server.Smuggler.Documents
{
    public class SnapshotDatabaseDestination : DatabaseDestination
    {
        private readonly Dictionary<string, SubscriptionState> _subscriptions;
        private readonly DocumentDatabase _database;

        public SnapshotDatabaseDestination(DocumentDatabase database, Dictionary<string, SubscriptionState> subscriptions, CancellationToken token = default) : base(database, token)
        {
            _subscriptions = subscriptions;
            _database = database;
        }

        public override ISubscriptionActions Subscriptions()
        {
            return new SnapshotSubscriptionActions(_database, _subscriptions);
        }

        protected class SnapshotSubscriptionActions : SubscriptionActions
        {
            private readonly Dictionary<string, SubscriptionState> _subscriptions;

            public SnapshotSubscriptionActions(DocumentDatabase database, Dictionary<string, SubscriptionState> subscriptions) : base(database)
            {
                _subscriptions = subscriptions;
            }

            protected override PutSubscriptionCommand CreatePutSubscriptionCommand(SubscriptionState subscriptionState)
            {
                if (_subscriptions.TryGetValue(SubscriptionState.Prefix + subscriptionState.SubscriptionName, out SubscriptionState oldState))
                {
                    var distance = ChangeVectorUtils.Distance(subscriptionState.ChangeVectorForNextBatchStartingPoint, oldState.ChangeVectorForNextBatchStartingPoint);
                    if (distance > 0)
                        subscriptionState.ChangeVectorForNextBatchStartingPoint = oldState.ChangeVectorForNextBatchStartingPoint;
                }
                else
                {
                    subscriptionState.ChangeVectorForNextBatchStartingPoint = null;
                }

                var command = new PutSubscriptionCommand(Database.Name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
                {
                    SubscriptionName = subscriptionState.SubscriptionName,
                    InitialChangeVector = subscriptionState.ChangeVectorForNextBatchStartingPoint,
                    Disabled = subscriptionState.Disabled

                };

                return command;
            }
        }
    }
}
