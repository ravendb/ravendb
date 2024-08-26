using System;
using System.Diagnostics;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class DeleteOngoingTaskCommand : UpdateDatabaseCommand
    {
        public long TaskId;
        public OngoingTaskType TaskType;

        public DeleteOngoingTaskCommand()
        {

        }

        public DeleteOngoingTaskCommand(long taskId, OngoingTaskType taskType, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            TaskType = taskType;
        }

        private long? _backupTaskIdToDelete;
        private string _hubNameToDelete;

        public override void AfterDatabaseRecordUpdate(ClusterOperationContext ctx, Table items, RavenAuditLogger clusterAuditLog)
        {
            switch (TaskType)
            {
                case OngoingTaskType.Backup:
                    if (_backupTaskIdToDelete == null)
                        return;

                    var responsibleNodeKey = ResponsibleNodeInfo.GenerateItemName(DatabaseName, _backupTaskIdToDelete.Value);
                    Delete(responsibleNodeKey);

                    var backupStatusKey = PeriodicBackupStatus.GenerateItemName(DatabaseName, _backupTaskIdToDelete.Value);
                    Delete(backupStatusKey);

                    void Delete(string key)
                    {
                        using (Slice.From(ctx.Allocator, key, out Slice _))
                        using (Slice.From(ctx.Allocator, key.ToLowerInvariant(), out Slice valueNameToDeleteLowered))
                        {
                            items.DeleteByKey(valueNameToDeleteLowered);
                        }
                    }

                    break;
                case OngoingTaskType.PullReplicationAsHub:
                    if (_hubNameToDelete == null)
                        return;
                    
                    if (clusterAuditLog.IsAuditEnabled)
                        clusterAuditLog.Audit($"Removed hub replication {_hubNameToDelete} in {DatabaseName} and all its certificates.");
                    
                    var certs = ctx.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.ReplicationCertificatesSchema, ClusterStateMachine.ReplicationCertificatesSlice);

                    using (Slice.From(ctx.Allocator, (this.DatabaseName + "/" + _hubNameToDelete + "/" ).ToLowerInvariant(), out var keySlice))
                    {
                        certs.DeleteByPrimaryKeyPrefix(keySlice);
                    }
                    break;
            }
        } 
        
        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Debug.Assert(TaskId != 0);

            switch (TaskType)
            {
                case OngoingTaskType.Replication:
                    var replicationTask = record.ExternalReplications?.Find(x => x.TaskId == TaskId);
                    if (replicationTask != null)
                    {
                        if (replicationTask.Name != null &&
                            replicationTask.Name.StartsWith(ServerWideExternalReplication.NamePrefix, StringComparison.OrdinalIgnoreCase))
                            throw new InvalidOperationException($"Can't delete task id: {TaskId}, name: '{replicationTask.Name}', " +
                                                                $"because it is a server-wide external replication task. Please use a dedicated operation.");
                        record.ExternalReplications.Remove(replicationTask);
                    }
                    break;
                case OngoingTaskType.PullReplicationAsHub:
                    var hubDefinition = record.HubPullReplications.Find(x => x.TaskId == TaskId);
                    if (hubDefinition != null)
                    {
                        _hubNameToDelete = hubDefinition.Name;
                        record.HubPullReplications.Remove(hubDefinition);
                    }
                    break;
                case OngoingTaskType.PullReplicationAsSink:
                    var pullTask = record.SinkPullReplications?.Find(x => x.TaskId == TaskId);
                    if (pullTask != null)
                    {
                        record.SinkPullReplications.Remove(pullTask);
                    }
                    break;
                case OngoingTaskType.Backup:
                    record.DeletePeriodicBackupConfiguration(TaskId);
                    _backupTaskIdToDelete = TaskId;
                    break;

                case OngoingTaskType.SqlEtl:
                    var sqlEtl = record.SqlEtls?.Find(x => x.TaskId == TaskId);
                    if (sqlEtl != null)
                    {
                        record.SqlEtls.Remove(sqlEtl);
                    }
                    break;

                case OngoingTaskType.RavenEtl:
                    var ravenEtl = record.RavenEtls?.Find(x => x.TaskId == TaskId);
                    if (ravenEtl != null)
                    {
                        record.RavenEtls.Remove(ravenEtl);
                    }
                    break;

                case OngoingTaskType.OlapEtl:
                    var olapEtl = record.OlapEtls?.Find(x => x.TaskId == TaskId);
                    if (olapEtl != null)
                    {
                        record.OlapEtls.Remove(olapEtl);
                    }
                    break;
                
                case OngoingTaskType.ElasticSearchEtl:
                    var elasticSearchEtl = record.ElasticSearchEtls.Find(x => x.TaskId == TaskId);
                    if (elasticSearchEtl != null)
                    {
                        record.ElasticSearchEtls.Remove(elasticSearchEtl);
                    }
                    break;
                case OngoingTaskType.QueueEtl:
                    var queueEtl = record.QueueEtls.Find(x => x.TaskId == TaskId);
                    if (queueEtl != null)
                    {
                        record.QueueEtls.Remove(queueEtl);
                    }
                    break;
                case OngoingTaskType.QueueSink:
                    var queueSink = record.QueueSinks.Find(x => x.TaskId == TaskId);
                    if (queueSink != null)
                    {
                        record.QueueSinks.Remove(queueSink);
                    }
                    break;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TypeConverter.ToBlittableSupportedType(TaskId);
            json[nameof(TaskType)] = TypeConverter.ToBlittableSupportedType(TaskType);
        }
    }
}
