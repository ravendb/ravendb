using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;

namespace Raven.Server.Smuggler.Documents.Actions;

public sealed class DatabaseSubscriptionActions : DatabaseSubscriptionActionsBase<PutSubscriptionCommand>
{
    public DatabaseSubscriptionActions(ServerStore serverStore, string name, BackupKind? backupKind = null, SmugglerResult result = null) : base(serverStore, name, backupKind, result)
    {
    }

    protected override PutSubscriptionCommand CreatePutSubscriptionCommand(SubscriptionState subscriptionState)
    {
        var command = new PutSubscriptionCommand(_name, subscriptionState.Query, null, RaftIdGenerator.DontCareId)
        {
            //After restore/export , subscription will start from the start
            InitialChangeVector = null
        };
        command.CopySettingsFrom(subscriptionState, ShouldKeepMentorNode(subscriptionState));

        return command;
    }

    protected override async ValueTask SendCommandsAsync()
    {
        await _serverStore.SendToLeaderAsync(new PutSubscriptionBatchCommand(_subscriptionCommands, RaftIdGenerator.DontCareId));
        _subscriptionCommands.Clear();
    }
}
