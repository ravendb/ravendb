using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.TransactionMerger.Commands
{
    public sealed class UpdateLocalBackupStatusCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly ClusterStateMachine _cluster;
        private readonly DynamicJsonValue _backupStatusAsJson;
        private readonly string _databaseName;
        private readonly long _taskId;

        public bool Inserted { get; private set; }

        public UpdateLocalBackupStatusCommand(ClusterStateMachine cluster, PeriodicBackupStatus backupStatus, string databaseName, long taskId)
        {
            _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            _backupStatusAsJson = (backupStatus ?? throw new ArgumentNullException(nameof(backupStatus))).ToJson();
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _taskId = taskId;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            if (_cluster.DatabaseExists(context, _databaseName) == false)
            {
                Inserted = false;
                return 0;
            }

            var statusBlittable = context.ReadObject(_backupStatusAsJson, $"backup-status-update-taskId-{_taskId}", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            BackupStatusStorage.Insert(context, statusBlittable, _databaseName, _taskId);
            Inserted = true;
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
