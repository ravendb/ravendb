using System.Collections.Generic;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public sealed class UpdateBackupHistoryCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly IReadOnlyList<BackupHistoryUpdate> _updates;

    public UpdateBackupHistoryCommand(IReadOnlyList<BackupHistoryUpdate> updates)
    {
        _updates = updates;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        foreach (var update in _updates)
        {
            var entry = new BackupHistoryEntry(update.Status);
            BackupHistoryStorage.StoreBackupStatus(context, update.DatabaseName, update.Status.TaskId, entry, update.CutoffTime);

            if (update.Result != null)
                BackupHistoryStorage.StoreBackupResultDetails(context, update.DatabaseName, update.Status.TaskId, entry, update.Result);
        }

        return _updates.Count;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, UpdateBackupHistoryCommand> ToDto(ClusterOperationContext context) =>
        new UpdateBackupHistoryCommandDto(_updates);

    private sealed record UpdateBackupHistoryCommandDto(IReadOnlyList<BackupHistoryUpdate> Updates)
        : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, UpdateBackupHistoryCommand>
    {
        public UpdateBackupHistoryCommand ToCommand(ClusterOperationContext context, DocumentDatabase database) =>
            new(Updates);
    }
}
