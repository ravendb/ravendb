using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class ToggleTaskStateCommand : UpdateDatabaseCommand
    {
        public long TaskId;
        public OngoingTaskType TaskType;
        public bool Disable;

        public ToggleTaskStateCommand() : base(null)
        {

        }

        public ToggleTaskStateCommand(long taskId, OngoingTaskType type, bool disable, string databaseName) : base(databaseName)
        {
            TaskId = taskId;
            TaskType = type;
            Disable = disable;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
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
                    var pullAsHub = record?.HubPullReplications?.Values.First(x => x.TaskId == TaskId);
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

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TypeConverter.ToBlittableSupportedType(TaskId);
            json[nameof(TaskType)] = TypeConverter.ToBlittableSupportedType(TaskType);
            json[nameof(Disable)] = TypeConverter.ToBlittableSupportedType(Disable);
        }
    }
}
