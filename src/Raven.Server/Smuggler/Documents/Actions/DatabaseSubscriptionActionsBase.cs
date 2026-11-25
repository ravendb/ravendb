using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
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

    private readonly BackupKind? _backupKind;
    private readonly SmugglerResult _result;

    protected DatabaseSubscriptionActionsBase(ServerStore serverStore, string name, BackupKind? backupKind = null, SmugglerResult result = null)
    {
        _serverStore = serverStore;
        _name = name;
        _backupKind = backupKind;
        _result = result;
    }

    /// <summary>
    /// A restore brings back the same database, so the subscription keeps its mentor node. An import usually targets
    /// another cluster, where the same node tag can belong to a different node, so the mentor node is dropped.
    /// </summary>
    protected bool ShouldKeepMentorNode(SubscriptionState subscriptionState)
    {
        if (_backupKind is BackupKind.Full or BackupKind.Incremental)
            return true;

        if (string.IsNullOrEmpty(subscriptionState.MentorNode) == false)
            _result?.AddWarning($"Subscription '{subscriptionState.SubscriptionName}' was imported without its mentor node '{subscriptionState.MentorNode}'" +
                                $"{(subscriptionState.PinToMentorNode ? " and without being pinned to it" : string.Empty)}, " +
                                "because the node tag refers to a node of the cluster it was exported from.");

        return false;
    }
    protected abstract T CreatePutSubscriptionCommand(SubscriptionState subscriptionState);

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
