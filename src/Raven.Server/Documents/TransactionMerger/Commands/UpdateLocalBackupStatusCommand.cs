using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.TransactionMerger.Commands
{
    public sealed class UpdateLocalBackupStatusCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly DynamicJsonValue _backupStatusAsJson;
        private readonly string _databaseName;
        private readonly string _dbId;
        private readonly long _taskId;

        public UpdateLocalBackupStatusCommand(PeriodicBackupStatus backupStatus, string databaseName, string dbId, long taskId)
        {
            _backupStatusAsJson = (backupStatus ?? throw new ArgumentNullException(nameof(backupStatus))).ToJson();
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _dbId = dbId;
            _taskId = taskId;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            var statusBlittable = context.ReadObject(_backupStatusAsJson, $"backup-status-update-taskId-{_taskId}", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            BackupStatusStorage.InsertBackupStatusBlittable(context, statusBlittable, _databaseName, _dbId, _taskId);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
