using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.Smuggler.Documents.Data;

namespace Raven.Server.Smuggler.Documents.Actions;

public abstract class DatabaseSubscriptionActionsBase<T> : ISubscriptionActions
    where T : PutSubscriptionCommand
{
    protected readonly ServerStore _serverStore;
    protected readonly string _name;
    protected readonly List<T> _subscriptionCommands = new List<T>();

    private static readonly int _batchSize = 1024;

    protected DatabaseSubscriptionActionsBase(ServerStore serverStore, string name)
    {
        _serverStore = serverStore;
        _name = name;
    }

    public abstract T CreatePutSubscriptionCommand(SubscriptionState subscriptionState);

    protected abstract ValueTask SendCommandsAsync();

    public virtual async ValueTask WriteSubscriptionAsync(SubscriptionState subscriptionState)
    {
        var cmd = CreatePutSubscriptionCommand(subscriptionState);
        _subscriptionCommands.Add(cmd);

        if (_subscriptionCommands.Count < _batchSize)
            return;

        await SendCommandsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_subscriptionCommands.Count == 0)
            return;

        await SendCommandsAsync();
    }
}

public class ShardedDatabaseSubscriptionActions : DatabaseSubscriptionActionsBase<PutShardedSubscriptionCommand>
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
            InitialChangeVector = null
        };
    }

    protected override async ValueTask SendCommandsAsync()
    {
        await _serverStore.SendToLeaderAsync(new PutShardedSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
        _subscriptionCommands.Clear();
    }
}

public class DatabaseSubscriptionActions : DatabaseSubscriptionActionsBase<PutSubscriptionCommand>
{
    public DatabaseSubscriptionActions(ServerStore serverStore, string name) : base(serverStore, name)
    {
    }

    public override PutSubscriptionCommand CreatePutSubscriptionCommand(SubscriptionState subscriptionState)
    {
        return new PutSubscriptionCommand(_name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
        {
            SubscriptionName = subscriptionState.SubscriptionName,
            //After restore/export , subscription will start from the start
            InitialChangeVector = null
        };
    }

    protected override async ValueTask SendCommandsAsync()
    {
        await _serverStore.SendToLeaderAsync(new PutSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
        _subscriptionCommands.Clear();
    }
}
