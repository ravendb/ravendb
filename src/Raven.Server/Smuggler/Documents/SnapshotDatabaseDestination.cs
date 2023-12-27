using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.Smuggler.Documents.Actions;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;

namespace Raven.Server.Smuggler.Documents
{
    public class SnapshotDatabaseDestination : DatabaseDestination
    {
        private readonly Dictionary<string, SubscriptionState> _subscriptions;

        public SnapshotDatabaseDestination(DocumentDatabase database, Dictionary<string, SubscriptionState> subscriptions, CancellationToken token = default)
            : base(database, token)
        {
            _subscriptions = subscriptions;
        }

        public override ISubscriptionActions Subscriptions()
        {
            return new SnapshotSubscriptionActions(_database, _subscriptions);
        }

        protected class SnapshotSubscriptionActions : DatabaseSubscriptionActionsBase<PutSubscriptionCommand>
        {
            private readonly Dictionary<string, SubscriptionState> _subscriptions;

            public SnapshotSubscriptionActions(DocumentDatabase database, Dictionary<string, SubscriptionState> subscriptions) : base(database.ServerStore, database.Name)
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

                var command = new PutSubscriptionCommand(_name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
                {
                    SubscriptionName = subscriptionState.SubscriptionName,
                    InitialChangeVector = subscriptionState.ChangeVectorForNextBatchStartingPoint,
                    Disabled = subscriptionState.Disabled
                };
                return command;
            }

            protected override async ValueTask SendCommandsAsync()
            {
                await _serverStore.SendToLeaderAsync(new PutSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
                _subscriptionCommands.Clear();
            }
        }
    }
}
