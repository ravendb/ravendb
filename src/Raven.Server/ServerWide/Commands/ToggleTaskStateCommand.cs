using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ToggleTaskStateCommand : UpdateDatabaseCommand
    {
        public long TaskId;
        public OngoingTaskType TaskType;
        public bool Disable;

        public ToggleTaskStateCommand()
        {
            // for deserialization
        }

        public ToggleTaskStateCommand(long taskId, OngoingTaskType type, bool disable, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            TaskType = type;
            Disable = disable;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            Debug.Assert(TaskId != 0);

            switch (TaskType)
            {
                case OngoingTaskType.Replication:

                    var watcher = record?.ExternalReplications.Find(x => x.TaskId == TaskId);
                    if (watcher != null)
                    {
                        ThrowIfServerWideTask(watcher.Name, ServerWideExternalReplication.NamePrefix, "external replication");

                        watcher.Disabled = Disable;
                        record.ClusterState.LastReplicationsIndex = index;
                    }
                    break;

                case OngoingTaskType.Backup:

                    var backup = record?.PeriodicBackups?.Find(x => x.TaskId == TaskId);
                    if (backup != null)
                    {
                        ThrowIfServerWideTask(backup.Name, ServerWideBackupConfiguration.NamePrefix, "backup");

                        backup.Disabled = Disable;
                        record.ClusterState.LastPeriodicBackupsIndex = index;
                    }
                    break;

                case OngoingTaskType.SqlEtl:

                    var sqlEtl = record?.SqlEtls?.Find(x => x.TaskId == TaskId);
                    if (sqlEtl != null)
                    {
                        sqlEtl.Disabled = Disable;
                        record.ClusterState.LastSqlEtlsIndex = index;
                    }
                    break;

                case OngoingTaskType.OlapEtl:

                    var olapEtl = record?.OlapEtls?.Find(x => x.TaskId == TaskId);
                    if (olapEtl != null)
                    {
                        olapEtl.Disabled = Disable;
                        record.ClusterState.LastOlapEtlsIndex = index;
                    }
                    break;

                case OngoingTaskType.RavenEtl:

                    var ravenEtl = record?.RavenEtls?.Find(x => x.TaskId == TaskId);
                    if (ravenEtl != null)
                    {
                        ravenEtl.Disabled = Disable;
                        record.ClusterState.LastRavenEtlsIndex = index;
                    }
                    break;

                case OngoingTaskType.PullReplicationAsHub:
                    var pullAsHub = record?.HubPullReplications?.First(x => x.TaskId == TaskId);
                    if (pullAsHub != null)
                    {
                        pullAsHub.Disabled = Disable;
                        record.ClusterState.LastReplicationsIndex = index;
                    }

                    break;

                case OngoingTaskType.PullReplicationAsSink:
                    var pullAsSink = record?.SinkPullReplications?.First(x => x.TaskId == TaskId);
                    if (pullAsSink != null)
                    {
                        pullAsSink.Disabled = Disable;
                        record.ClusterState.LastReplicationsIndex = index;
                    }

                    break;
            }


            void ThrowIfServerWideTask(string name, string prefix, string typeName)
            {
                if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    var action = Disable ? "disable" : "enable";
                    throw new InvalidOperationException($"Can't {action} task name '{name}', because it is a server-wide {typeName} task");
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TypeConverter.ToBlittableSupportedType(TaskId);
            json[nameof(TaskType)] = TypeConverter.ToBlittableSupportedType(TaskType);
            json[nameof(Disable)] = TypeConverter.ToBlittableSupportedType(Disable);
        }
    }
}
