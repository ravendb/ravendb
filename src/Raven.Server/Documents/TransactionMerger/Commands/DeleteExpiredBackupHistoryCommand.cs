using System;
using System.Collections.Generic;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public sealed class DeleteExpiredBackupHistoryCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly string _databaseName;
    private readonly DateTime _cutoffTime;
    private readonly HashSet<long> _tasksToKeepNewestFullBackup;
    private readonly int _maxEntriesToDelete;

    public DeleteExpiredBackupHistoryCommand(string databaseName, DateTime cutoffTime, HashSet<long> tasksToKeepNewestFullBackup, int maxEntriesToDelete)
    {
        _databaseName = databaseName;
        _cutoffTime = cutoffTime;
        _tasksToKeepNewestFullBackup = tasksToKeepNewestFullBackup;
        _maxEntriesToDelete = maxEntriesToDelete;
    }

    public int DeletedEntries { get; private set; }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        DeletedEntries = BackupHistoryStorage.DeleteExpiredEntries(context, _databaseName, _cutoffTime, _tasksToKeepNewestFullBackup, _maxEntriesToDelete);
        return DeletedEntries;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, DeleteExpiredBackupHistoryCommand> ToDto(ClusterOperationContext context) =>
        new DeleteExpiredBackupHistoryCommandDto(_databaseName, _cutoffTime, _tasksToKeepNewestFullBackup, _maxEntriesToDelete);

    private sealed record DeleteExpiredBackupHistoryCommandDto(string DatabaseName, DateTime CutoffTime, HashSet<long> TasksToKeepNewestFullBackup, int MaxEntriesToDelete)
        : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, DeleteExpiredBackupHistoryCommand>
    {
        public DeleteExpiredBackupHistoryCommand ToCommand(ClusterOperationContext context, DocumentDatabase database) =>
            new(DatabaseName, CutoffTime, TasksToKeepNewestFullBackup, MaxEntriesToDelete);
    }
}
