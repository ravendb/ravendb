using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public sealed class AdminServerWideHandler : ServerRequestHandler
    {
        [RavenAction("/admin/configuration/server-wide", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task GetConfigurationServerWide()
        {
            // FullPath removes the trailing '/' so adding it back for the studio
            var localRootPath = ServerStore.Configuration.Backup.LocalRootPath;
            var localRootFullPath = localRootPath != null ? localRootPath.FullPath + Path.DirectorySeparatorChar : null;

            var result = new DynamicJsonValue
            {
                [nameof(ServerStore.Configuration.Backup.LocalRootPath)] = localRootFullPath,
                [nameof(ServerStore.Configuration.Backup.AllowedAwsRegions)] = ServerStore.Configuration.Backup.AllowedAwsRegions,
                [nameof(ServerStore.Configuration.Backup.AllowedDestinations)] = ServerStore.Configuration.Backup.AllowedDestinations,
            };

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result);
            }
        }

        // Used for Create, Edit
        [RavenAction("/admin/configuration/server-wide/backup", "PUT", AuthorizationStatus.ClusterAdmin)]
        public async Task PutServerWideBackupConfigurationCommand()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "server-wide-backup-configuration");
                var configuration = JsonDeserializationCluster.ServerWideBackupConfiguration(configurationBlittable);

                ServerStore.LicenseManager.AssertCanAddPeriodicBackup(configuration);
                BackupConfigurationHelper.UpdateLocalPathIfNeeded(configuration, ServerStore);
                BackupConfigurationHelper.AssertBackupConfiguration(configuration, ServerStore.Configuration.Backup);
                BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(configuration, ServerStore);

                var (newIndex, _) = await ServerStore.PutServerWideBackupConfigurationAsync(configuration, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(newIndex);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var backupName = ServerStore.Cluster.GetServerWideTaskNameByTaskId(context, ClusterStateMachine.ServerWideConfigurationKey.Backup, newIndex);
                    if (backupName == null)
                        throw new InvalidOperationException($"Backup name is null for server-wide backup with task id: {newIndex}");

                    var putResponse = new PutServerWideBackupConfigurationResponse
                    {
                        Name = backupName,
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    context.Write(writer, putResponse.ToJson());
                }
            }
        }

        [RavenAction("/admin/configuration/server-wide/external-replication", "PUT", AuthorizationStatus.ClusterAdmin)]
        public async Task PutServerWideExternalReplicationCommand()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "server-wide-external-replication-configuration");
                var configuration = JsonDeserializationCluster.ServerWideExternalReplication(configurationBlittable);

                ServerStore.LicenseManager.AssertCanAddExternalReplication(configuration.DelayReplicationFor);

                var (newIndex, _) = await ServerStore.PutServerWideExternalReplicationAsync(configuration, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(newIndex);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var taskName = ServerStore.Cluster.GetServerWideTaskNameByTaskId(context, ClusterStateMachine.ServerWideConfigurationKey.ExternalReplication, newIndex);
                    if (taskName == null)
                        throw new InvalidOperationException($"External replication name is null for server-wide external replication with task id: {newIndex}");

                    var putResponse = new ServerWideExternalReplicationResponse
                    {
                        Name = taskName,
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    context.Write(writer, putResponse.ToJson());
                }
            }
        }

        [RavenAction("/admin/configuration/server-wide/backup", "DELETE", AuthorizationStatus.ClusterAdmin)]
        public async Task DeleteServerWideBackupConfigurationCommand()
        {
            // backward compatibility
            await DeleteServerWideTaskCommand(OngoingTaskType.Backup);
        }

        [RavenAction("/admin/configuration/server-wide/task", "DELETE", AuthorizationStatus.ClusterAdmin)]
        public async Task DeleteServerWideTaskCommand()
        {
            var typeAsString = GetStringQueryString("type", required: true);

            if (Enum.TryParse(typeAsString, out OngoingTaskType type) == false)
                throw new ArgumentException($"{typeAsString} is unknown task type.");

            await DeleteServerWideTaskCommand(type);
        }

        [RavenAction("/admin/configuration/server-wide/backup", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetServerWideBackupConfigurations()
        {
            // backward compatibility
            return GetTaskConfigurationsAsync(OngoingTaskType.Backup, JsonDeserializationCluster.ServerWideBackupConfiguration);
        }

        [RavenAction("/admin/configuration/server-wide/tasks", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task GetServerWideTasks()
        {
            var typeAsString = GetStringQueryString("type", required: true);
            if (Enum.TryParse(typeAsString, out OngoingTaskType type) == false)
                throw new ArgumentException($"{typeAsString} is unknown task type.");

            Func<BlittableJsonReaderObject, IDynamicJson> converter;
            switch (type)
            {
                case OngoingTaskType.Backup:
                    converter = JsonDeserializationCluster.ServerWideBackupConfiguration;
                    break;

                case OngoingTaskType.Replication:
                    converter = JsonDeserializationCluster.ServerWideExternalReplication;
                    break;

                default:
                    throw new ArgumentOutOfRangeException($"Task type '{type} isn't suppported");
            }

            await GetTaskConfigurationsAsync(type, converter);
        }

        [RavenAction("/admin/configuration/server-wide/state", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task ToggleServerWideTaskState()
        {
            var typeAsString = GetStringQueryString("type", required: true);
            var taskName = GetStringQueryString("name", required: true);
            var disable = GetBoolValueQueryString("disable") ?? true;

            if (Enum.TryParse(typeAsString, out OngoingTaskType type) == false)
                throw new ArgumentException($"{typeAsString} is unknown task type.");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var configuration = new ToggleServerWideTaskStateCommand.Parameters
                {
                    Type = type,
                    TaskName = taskName,
                    Disable = disable
                };
                var (newIndex, _) = await ServerStore.ToggleServerWideTaskStateAsync(configuration, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(newIndex);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var toggleResponse = new ServerWideTaskResponse
                    {
                        Name = taskName,
                        RaftCommandIndex = newIndex
                    };

                    context.Write(writer, toggleResponse.ToJson());
                }
            }
        }

        [RavenAction("/admin/configuration/server-wide/connection-strings", "PUT", AuthorizationStatus.ClusterAdmin)]
        public async Task PutServerWideConnectionString()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var connectionStringBlittable = await context.ReadForMemoryAsync(RequestBodyStream(), "server-wide-connection-string");
                var serverWideConnectionString = ServerWideConnectionString.FromBlittable(connectionStringBlittable);

                if (serverWideConnectionString?.ConnectionString == null)
                    throw new BadRequestException("Connection string is missing or invalid");

                var errors = new List<string>();
                serverWideConnectionString.ConnectionString.Validate(errors);
                if (errors.Count > 0)
                    throw new BadRequestException($"Invalid connection string configuration. Errors: {string.Join($"{Environment.NewLine}", errors)}");

                var (newIndex, _) = await ServerStore.PutServerWideConnectionStringAsync(serverWideConnectionString, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(newIndex);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var putResponse = new PutServerWideConnectionStringResult
                    {
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(PutServerWideConnectionStringResult.RaftCommandIndex)] = putResponse.RaftCommandIndex
                    });
                }
            }
        }

        [RavenAction("/admin/configuration/server-wide/connection-strings", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task GetServerWideConnectionStrings()
        {
            var name = GetStringQueryString("connectionStringName", required: false);
            var typeAsString = GetStringQueryString("type", required: false);

            ConnectionStringType? type = null;
            if (name != null)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new BadRequestException($"'{nameof(name)}' must have a non empty value");
            }

            if (typeAsString != null)
                type = ParseConnectionStringType(typeAsString);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var blittables = ServerStore.Cluster.GetServerWideConnectionStrings(context, type, name);
                var result = new GetServerWideConnectionStringsResult();

                foreach (var blittable in blittables)
                {
                    var connectionString = ServerWideConnectionString.FromBlittable(blittable);
                    if (connectionString != null)
                        result.Results.Add(connectionString);
                }

                var allDatabases = ServerStore.Cluster.GetAllDatabases(context);
                var usageMap = BuildUsageMap(allDatabases);

                foreach (var cs in result.Results)
                {
                    var prefixedName = ServerWideConnectionString.GetDatabaseRecordConnectionStringName(cs.Name);
                    if (usageMap.TryGetValue(prefixedName, out var usages))
                        cs.UsedByTasks = usages;
                }

                context.Write(writer, result.ToJson());
            }
        }

        private static Dictionary<string, List<ConnectionStringTaskUsage>> BuildUsageMap(
            List<DatabaseRecord> databases)
        {
            var map = new Dictionary<string, List<ConnectionStringTaskUsage>>(StringComparer.OrdinalIgnoreCase);

            void Add(string connectionStringName, long taskId, string taskName)
            {
                if (connectionStringName == null)
                    return;
                if (connectionStringName.StartsWith(ServerWideConnectionString.NamePrefix, StringComparison.OrdinalIgnoreCase) == false)
                    return;
                if (map.TryGetValue(connectionStringName, out var list) == false)
                    map[connectionStringName] = list = new List<ConnectionStringTaskUsage>();
                list.Add(new ConnectionStringTaskUsage { TaskId = taskId, TaskName = taskName });
            }

            foreach (var db in databases)
            {
                foreach (var t in db.RavenEtls)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.SqlEtls)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.OlapEtls)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.ElasticSearchEtls)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.QueueEtls)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.SnowflakeEtls)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.QueueSinks)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.EmbeddingsGenerations)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.ExternalReplications)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
                foreach (var t in db.SinkPullReplications)
                    Add(t.ConnectionStringName, t.TaskId, t.Name);
            }

            return map;
        }

        [RavenAction("/admin/configuration/server-wide/connection-strings", "DELETE", AuthorizationStatus.ClusterAdmin)]
        public async Task RemoveServerWideConnectionString()
        {
            var name = GetStringQueryString("connectionString", required: true);
            var typeAsString = GetStringQueryString("type", required: true);

            var type = ParseConnectionStringType(typeAsString);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var deleteConfiguration = new RemoveServerWideConnectionStringCommand.DeleteConfiguration
                {
                    ConnectionStringName = name,
                    Type = type
                };

                var (newIndex, _) = await ServerStore.RemoveServerWideConnectionStringAsync(deleteConfiguration, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(newIndex);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var removeResponse = new RemoveServerWideConnectionStringResult
                    {
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(RemoveServerWideConnectionStringResult.RaftCommandIndex)] = removeResponse.RaftCommandIndex
                    });
                }
            }
        }

        private static ConnectionStringType ParseConnectionStringType(string typeAsString)
        {
            if (Enum.TryParse(typeAsString, true, out ConnectionStringType type) == false)
                throw new BadRequestException($"Unknown connection string type: {typeAsString}");

            return type;
        }

        private async Task DeleteServerWideTaskCommand(OngoingTaskType taskType)
        {
            var name = GetStringQueryString("name", required: true);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var deleteConfiguration = new DeleteServerWideTaskCommand.DeleteConfiguration
                {
                    TaskName = name,
                    Type = taskType
                };

                var (newIndex, _) = await ServerStore.DeleteServerWideTaskAsync(deleteConfiguration, GetRaftRequestIdFromQuery());
                await ServerStore.Cluster.WaitForIndexNotification(newIndex);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var deleteResponse = new ServerWideTaskResponse
                    {
                        Name = name,
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                    context.Write(writer, deleteResponse.ToJson());
                }
            }
        }

        private async Task GetTaskConfigurationsAsync<T>(OngoingTaskType type, Func<BlittableJsonReaderObject, T> converter)
            where T : IDynamicJson
        {
            var taskName = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var blittables = ServerStore.Cluster.GetServerWideConfigurations(context, type, taskName);
                var result = new ServerWideTasksResult<T>();

                foreach (var blittable in blittables)
                {
                    var configuration = converter(blittable);
                    result.Results.Add(configuration);
                }

                context.Write(writer, result.ToJson());
            }
        }
    }

    public sealed class ServerWideTasksResult<T> : IDynamicJson
        where T : IDynamicJson
    {
        public List<T> Results;

        public ServerWideTasksResult()
        {
            Results = new List<T>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()))
            };
        }
    }
}