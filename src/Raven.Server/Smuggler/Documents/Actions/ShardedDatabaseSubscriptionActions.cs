using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Commands.Subscriptions;

namespace Raven.Server.Smuggler.Documents.Actions;

public sealed class ShardedDatabaseSubscriptionActions : DatabaseSubscriptionActionsBase<PutShardedSubscriptionCommand>
{
    public ShardedDatabaseSubscriptionActions(ServerStore serverStore, string name) : base(serverStore, name)
    {
    }

    public override PutShardedSubscriptionCommand CreatePutSubscriptionCommand(SubscriptionState subscriptionState)
    {
        return new PutShardedSubscriptionCommand(_name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
        {
            SubscriptionName = subscriptionState.SubscriptionName,
            //After restore/export , subscription will start from the start
            InitialChangeVector = null,
            ArchivedDataProcessingBehavior = subscriptionState.ArchivedDataProcessingBehavior
        };
    }

    protected override async ValueTask SendCommandsAsync()
    {
        await _serverStore.SendToLeaderAsync(new PutShardedSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
        _subscriptionCommands.Clear();
    }
}
