using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
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

        }

        public ToggleTaskStateCommand(long taskId, OngoingTaskType type, bool disable, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            TaskType = type;
            Disable = disable;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Debug.Assert(TaskId != 0);

            switch (TaskType)
            {
                case OngoingTaskType.Replication:

                    var watcher = record?.ExternalReplications.Find(x => x.TaskId == TaskId);
                    if (watcher != null)
                    {
                        watcher.Disabled = Disable;
                    }
                    break;

                case OngoingTaskType.Backup:

                    var backup = record?.PeriodicBackups?.Find(x => x.TaskId == TaskId);
                    if (backup != null)
                    {
                        if (backup.Name.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase))
                        {
                            var action = Disable ? "disable" : "enable";
                            throw new InvalidOperationException($"Can't {action} task name '{backup.Name}', because it is a server-wide backup task");
                        }

                        backup.Disabled = Disable;
                    }
                    break;

                case OngoingTaskType.SqlEtl:

                    var sqlEtl = record?.SqlEtls?.Find(x => x.TaskId == TaskId);
                    if (sqlEtl != null)
                    {
                        sqlEtl.Disabled = Disable;
                    }
                    break;

                case OngoingTaskType.RavenEtl:

                    var ravenEtl = record?.RavenEtls?.Find(x => x.TaskId == TaskId);
                    if (ravenEtl != null)
                    {
                        ravenEtl.Disabled = Disable;
                    }
                    break;
                
                case OngoingTaskType.PullReplicationAsHub:
                    var pullAsHub = record?.HubPullReplications?.First(x => x.TaskId == TaskId);
                    if (pullAsHub != null)
                    {
                        pullAsHub.Disabled = Disable;
                    }

                    break;
                
                case OngoingTaskType.PullReplicationAsSink:
                    var pullAsSink = record?.SinkPullReplications?.First(x => x.TaskId == TaskId);
                    if (pullAsSink != null)
                    {
                        pullAsSink.Disabled = Disable;
                    }

                    break;
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
