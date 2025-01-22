using System;
using System.Collections.Generic;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands
{
    public sealed class DeleteLocalBackupStatusesCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly string _databaseName;
        private readonly string _dbId;
        private readonly HashSet<long> _taskIds;

        public DeleteLocalBackupStatusesCommand(HashSet<long> taskIds, string databaseName, string dbId)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _dbId = dbId ?? throw new ArgumentNullException(nameof(dbId));
            _taskIds = taskIds ?? throw new ArgumentNullException(nameof(taskIds));
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            foreach (var taskId in _taskIds)
            {
                BackupStatusStorage.DeleteBackupStatus(context, _databaseName, _dbId, taskId);
            }
            return _taskIds.Count;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
