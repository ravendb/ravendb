using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public sealed class UpdateBackupHistoryCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly string _databaseName;
    private readonly PeriodicBackupStatus _status;
    private readonly BackupResult _result;
    private readonly DateTime _cutoffTime;

    public UpdateBackupHistoryCommand(string databaseName, PeriodicBackupStatus status, BackupResult result, DateTime cutoffTime)
    {
        _databaseName = databaseName;
        _status = status;
        _result = result;
        _cutoffTime = cutoffTime;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        BackupHistoryStorage.StoreBackupStatus(context, _databaseName, _status, _cutoffTime);
        BackupHistoryStorage.StoreBackupResultDetails(context, _databaseName, _status, _result);

        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        throw new NotImplementedException();
    }
}
