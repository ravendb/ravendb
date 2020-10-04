using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminStudioServerWideHandler : ServerRequestHandler
    {
        [RavenAction("/admin/server-wide/tasks", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetServerWideTasksForStudio()
        {
            var taskName = GetStringQueryString("name", required: false);
            var typeAsString = GetStringQueryString("type", required: false);
            var tryParse = Enum.TryParse(typeAsString, out OngoingTaskType type);

            if (typeAsString != null && tryParse == false)
                throw new ArgumentException($"{typeAsString} is unknown task type.");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var result = new ServerWideTasksResult();

                var blittables = ServerStore.Cluster.GetServerWideConfigurations(context, OngoingTaskType.Backup, taskName);
                foreach (var blittable in blittables)
                {
                    var configuration = JsonDeserializationCluster.ServerWideBackupConfiguration(blittable);
                    
                    if (taskName != null && type == OngoingTaskType.Backup && 
                        string.Equals(taskName, configuration.Name))
                        continue;

                    result.Tasks.Add(new ServerWideTasksResult.ServerWideBackupTask
                    {
                        TaskId = configuration.TaskId,
                        TaskName = configuration.Name,
                        TaskState = configuration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                        ExcludedDatabases = configuration.ExcludedDatabases,
                        BackupType = configuration.BackupType,
                        RetentionPolicy = configuration.RetentionPolicy,
                        BackupDestinations = configuration.GetDestinations(),
                        IsEncrypted = configuration.BackupEncryptionSettings != null &&
                                      configuration.BackupEncryptionSettings.EncryptionMode != EncryptionMode.None
                    });
                }

                blittables = ServerStore.Cluster.GetServerWideConfigurations(context, OngoingTaskType.Replication, taskName);
                foreach (var blittable in blittables)
                {
                    var configuration = JsonDeserializationCluster.ServerWideExternalReplication(blittable);

                    if (taskName != null && type == OngoingTaskType.Replication &&
                        string.Equals(taskName, configuration.Name))
                        continue;

                    result.Tasks.Add(new ServerWideTasksResult.ServerWideExternalReplicationTask
                    {
                        TaskId = configuration.TaskId,
                        TaskName = configuration.Name,
                        TaskState = configuration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                        ExcludedDatabases = configuration.ExcludedDatabases,
                        TopologyDiscoveryUrls = configuration.TopologyDiscoveryUrls,
                        DelayReplicationFor = configuration.DelayReplicationFor,
                    });
                }

                context.Write(writer, result.ToJson());
                writer.Flush();

                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/server-wide/backup-data-directory", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task FullBackupDataDirectory()
        {
            var path = GetStringQueryString("path", required: true);
            var requestTimeoutInMs = GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;
            var getNodesInfo = GetBoolValueQueryString("getNodesInfo", required: false) ?? false;

            var pathSetting = new PathSetting(path);
            await BackupConfigurationHelper.GetFullBackupDataDirectory(pathSetting, databaseName: null, requestTimeoutInMs, getNodesInfo, ServerStore, ResponseBodyStream());
        }

        public class ServerWideTasksResult : IDynamicJson
        {
            public List<ServerWideTask> Tasks;

            public ServerWideTasksResult()
            {
                Tasks = new List<ServerWideTask>();
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Tasks)] = new DynamicJsonArray(Tasks.Select(x => x.ToJson()))
                };
            }

            public abstract class ServerWideTask : OngoingTask
            {
                public string[] ExcludedDatabases { get; set; }

                public override DynamicJsonValue ToJson()
                {
                    var json =  base.ToJson();
                    json[nameof(ExcludedDatabases)] = ExcludedDatabases;
                    return json;
                }
            }

            public class ServerWideBackupTask : ServerWideTask
            {
                public BackupType BackupType { get; set; }

                public List<string> BackupDestinations { get; set; }

                public RetentionPolicy RetentionPolicy { get; set; }

                public bool IsEncrypted { get; set; }

                public override DynamicJsonValue ToJson()
                {
                    var json = base.ToJson();
                    json[nameof(BackupType)] = BackupType;
                    json[nameof(BackupDestinations)] = new DynamicJsonArray(BackupDestinations);
                    json[nameof(RetentionPolicy)] = RetentionPolicy;
                    json[nameof(IsEncrypted)] = IsEncrypted;
                    return json;
                }
            }

            public class ServerWideExternalReplicationTask : ServerWideTask
            {
                public string[] TopologyDiscoveryUrls { get; set; }

                public TimeSpan DelayReplicationFor { get; set; }

                public override DynamicJsonValue ToJson()
                {
                    var json = base.ToJson();
                    json[nameof(TopologyDiscoveryUrls)] = new DynamicJsonArray(TopologyDiscoveryUrls);
                    json[nameof(DelayReplicationFor)] = DelayReplicationFor;
                    return json;
                }
            }
        }
    }
}
