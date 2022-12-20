using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.Smuggler.Documents.Data;

namespace Raven.Server.Smuggler.Documents.Actions;

public class DatabaseSubscriptionActions : ISubscriptionActions
{
    private readonly ServerStore _serverStore;
    private readonly string _name;
    private readonly List<PutSubscriptionCommand> _subscriptionCommands = new List<PutSubscriptionCommand>();

    public DatabaseSubscriptionActions(DocumentDatabase database)
    {
        _serverStore = database.ServerStore;
        _name = database.Name;
    }

    public DatabaseSubscriptionActions(ServerStore serverStore, string name)
    {
        _serverStore = serverStore;
        _name = name;
    }

    public async ValueTask DisposeAsync()
    {
        if (_subscriptionCommands.Count == 0)
            return;

        await SendCommandsAsync();
    }

    public async ValueTask WriteSubscriptionAsync(SubscriptionState subscriptionState)
    {
        const int batchSize = 1024;

        _subscriptionCommands.Add(new PutSubscriptionCommand(_name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
        {
            SubscriptionName = subscriptionState.SubscriptionName,
            //After restore/export , subscription will start from the start
            InitialChangeVector = null
        });

        if (_subscriptionCommands.Count < batchSize)
            return;

        await SendCommandsAsync();
    }

    private async ValueTask SendCommandsAsync()
    {
        await _serverStore.SendToLeaderAsync(new PutSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
        _subscriptionCommands.Clear();
    }
}
