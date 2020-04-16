// -----------------------------------------------------------------------
//  <copyright file="ServerWideBackupHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Rachis;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Config.Settings;
using Raven.Server.Json;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.Web.System
{
    public class AdminServerWideHandler : ServerRequestHandler
    {
        [RavenAction("/admin/configuration/server-wide", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetConfigurationServerWide()
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
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result);
            }

            return Task.CompletedTask;
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
                BackupConfigurationHelper.AssertBackupConfiguration(configuration);
                BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(configuration, ServerStore);

                var (newIndex, _) = await ServerStore.PutServerWideBackupConfigurationAsync(configuration, GetRaftRequestIdFromQuery());
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);
               
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var backupName = ServerStore.Cluster.GetServerWideTaskNameByTaskId(context, ClusterStateMachine.ServerWideConfigurationKey.Backup, newIndex);
                    if (backupName == null)
                        throw new InvalidOperationException($"Backup name is null for server-wide backup with task id: {newIndex}");
                    
                    var putResponse = new ServerWideTaskResponse
                    {
                        Name = backupName,
                        RaftCommandIndex = newIndex 
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    context.Write(writer, putResponse.ToJson());
                    writer.Flush();
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
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var taskName = ServerStore.Cluster.GetServerWideTaskNameByTaskId(context, ClusterStateMachine.ServerWideConfigurationKey.ExternalReplication, newIndex);
                    if (taskName == null)
                        throw new InvalidOperationException($"External replication name is null for server-wide external replication with task id: {newIndex}");

                    var putResponse = new ServerWideTaskResponse
                    {
                        Name = taskName,
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                    context.Write(writer, putResponse.ToJson());
                    writer.Flush();
                }
            }
        }

        [RavenAction("/admin/configuration/server-wide/backup", "DELETE", AuthorizationStatus.ClusterAdmin)]
        public async Task DeleteServerWideExternalReplicationConfigurationCommand()
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
            GetTaskConfigurations(OngoingTaskType.Backup, JsonDeserializationCluster.ServerWideBackupConfiguration);
            return Task.CompletedTask;
        }

        [RavenAction("/admin/configuration/server-wide/tasks", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetServerWideTasks()
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

            GetTaskConfigurations(type, converter);
            return Task.CompletedTask;
        }

        [RavenAction("/admin/configuration/server-wide/tasks-for-studio", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetServerWideTasksForStudio()
        {
            var taskName = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var result = new ServerWideTaskConfigurations();

                var blittables = ServerStore.Cluster.GetServerWideConfigurations(context, OngoingTaskType.Backup, taskName);
                foreach (var blittable in blittables)
                {
                    var backup = JsonDeserializationServer.ServerWideBackupConfigurationForStudio(blittable);
                    result.Backups.Add(backup);
                }

                blittables = ServerStore.Cluster.GetServerWideConfigurations(context, OngoingTaskType.Replication, taskName);
                foreach (var blittable in blittables)
                {
                    var externalReplication = JsonDeserializationCluster.ServerWideExternalReplication(blittable);
                    result.ExternalReplications.Add(externalReplication);
                }

                context.Write(writer, result.ToJson());
                writer.Flush();

                return Task.CompletedTask;
            }
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
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var toggleResponse = new ServerWideTaskResponse
                    {
                        Name = taskName,
                        RaftCommandIndex = newIndex 
                    };

                    context.Write(writer, toggleResponse.ToJson());
                    writer.Flush();
                }
            }
        }
        
        [RavenAction("/admin/backup-data-directory", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task FullBackupDataDirectory()
        {
            var path = GetStringQueryString("path", required: true);
            var requestTimeoutInMs = GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000; 
            var getNodesInfo = GetBoolValueQueryString("getNodesInfo", required: false) ?? false;

            var pathSetting = new PathSetting(path);
            await BackupConfigurationHelper.GetFullBackupDataDirectory(pathSetting, databaseName: null, requestTimeoutInMs, getNodesInfo, ServerStore, ResponseBodyStream());
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
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var deleteResponse = new ServerWideTaskResponse
                    {
                        Name = name,
                        RaftCommandIndex = newIndex
                    };

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                    context.Write(writer, deleteResponse.ToJson());
                    writer.Flush();
                }
            }
        }

        private void GetTaskConfigurations<T>(OngoingTaskType type, Func<BlittableJsonReaderObject, T> converter)
            where T : IDynamicJson
        {
            var taskName = GetStringQueryString("name", required: false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var blittables = ServerStore.Cluster.GetServerWideConfigurations(context, type, taskName);
                var result = new ServerWideTasksResult<T>();

                foreach (var blittable in blittables)
                {
                    var configuration = converter(blittable);
                    result.Results.Add(configuration);
                }

                context.Write(writer, result.ToJson());
                writer.Flush();
            }
        }
    }

    public class ServerWideTasksResult<T> : IDynamicJson
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

    public class ServerWideTaskConfigurations : IDynamicJson
    {
        public List<ServerWideBackupConfigurationForStudio> Backups;

        public List<ServerWideExternalReplication> ExternalReplications;

        public ServerWideTaskConfigurations()
        {
            Backups = new List<ServerWideBackupConfigurationForStudio>();
            ExternalReplications = new List<ServerWideExternalReplication>();
        }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Backups)] = new DynamicJsonArray(Backups.Select(x => x.ToJson())),
                [nameof(ExternalReplications)] = new DynamicJsonArray(ExternalReplications.Select(x => x.ToJson()))
            };
        }
    }
    
    public class ServerWideBackupConfigurationForStudio : ServerWideBackupConfiguration
    {
        public OngoingTaskState TaskState { get; set; }

        public List<string> BackupDestinations { get; set; }

        public bool IsEncrypted { get; set; }
        
        public ServerWideBackupConfigurationForStudio()
        {
            BackupDestinations = new List<string>();
        }
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(TaskState)] = TaskState;
            json[nameof(BackupDestinations)] = new DynamicJsonArray(BackupDestinations);
            json[nameof(IsEncrypted)] = IsEncrypted;
            return json;
        }
    }
}
