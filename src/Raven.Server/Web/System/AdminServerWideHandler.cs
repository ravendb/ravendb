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
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Rachis;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Config.Settings;
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
                    
                    var putResponse = new PutServerWideBackupConfigurationResponse
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

                ServerStore.LicenseManager.AssertCanAddExternalReplication();

                var (newIndex, _) = await ServerStore.PutServerWideExternalReplicationAsync(configuration, GetRaftRequestIdFromQuery());
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var taskName = ServerStore.Cluster.GetServerWideTaskNameByTaskId(context, ClusterStateMachine.ServerWideConfigurationKey.Backup, newIndex);
                    if (taskName == null)
                        throw new InvalidOperationException($"Backup name is null for server-wide backup with task id: {newIndex}");

                    var putResponse = new PutServerWideBackupConfigurationResponse
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
            await DeleteServerWideTaskCommand(ServerWide.Commands.DeleteServerWideTaskCommand.DeleteConfiguration.TaskType.Backup);
        }

        [RavenAction("/admin/configuration/server-wide/external-replication", "DELETE", AuthorizationStatus.ClusterAdmin)]
        public async Task DeleteServerWideBackupConfigurationCommand()
        {
            await DeleteServerWideTaskCommand(ServerWide.Commands.DeleteServerWideTaskCommand.DeleteConfiguration.TaskType.ExternalReplication);
        }

        // Get all server-wide backups -OR- specific task by the task name... 
        // todo : consider also returning each db specific details for the studio list view here
        [RavenAction("/admin/configuration/server-wide/backup", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetServerWideBackupConfigurationCommand()
        {
            var taskName = GetStringQueryString("name", required: false);
            
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var backups = ServerStore.Cluster.GetServerWideBackupConfigurations(context, taskName);
                ServerWideBackupConfigurationResults backupsResult = new ServerWideBackupConfigurationResults();
                
                foreach (var backupBlittable in backups)
                {
                    var backup = JsonDeserializationServer.ServerWideBackupConfiguration(backupBlittable);
                    backup.BackupDestinations = backup.GetFullBackupDestinations();
                    backup.IsEncrypted = backup.BackupEncryptionSettings != null &&
                                         backup.BackupEncryptionSettings.EncryptionMode != EncryptionMode.None;

                    backupsResult.Results.Add(backup);
                }
                
                context.Write(writer, backupsResult.ToJson());
                writer.Flush();

                return Task.CompletedTask;
            }
        }

        [RavenAction("/admin/configuration/server-wide/backup/state", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task ToggleServerWideBackupTaskState()
        {
            var disable = GetBoolValueQueryString("disable") ?? true;
            var taskName = GetStringQueryString("taskName", required: false);
           
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                // Get existing task
                var serveWideBackupBlittable = ServerStore.Cluster.GetServerWideBackupConfigurations(context, taskName).FirstOrDefault();
                if (serveWideBackupBlittable == null)
                    throw new InvalidOperationException($"Server-Wide Backup Task: {taskName} was not found in the server.");
               
                // Toggle
                ServerWideBackupConfiguration serverWideBackup = JsonDeserializationServer.ServerWideBackupConfiguration(serveWideBackupBlittable);
                serverWideBackup.Disabled = disable; 
            
                // Save task
                var (newIndex, _) = await ServerStore.PutServerWideBackupConfigurationAsync(serverWideBackup, GetRaftRequestIdFromQuery());
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var toggleResponse = new PutServerWideBackupConfigurationResponse()
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

        private async Task DeleteServerWideTaskCommand(DeleteServerWideTaskCommand.DeleteConfiguration.TaskType taskType)
        {
            var name = GetStringQueryString("name", required: true);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var deleteConfiguration = new DeleteServerWideTaskCommand.DeleteConfiguration
                {
                    Name = name,
                    Type = taskType
                };

                var (newIndex, _) = await ServerStore.DeleteServerWideTaskAsync(deleteConfiguration, GetRaftRequestIdFromQuery());
                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newIndex);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                using (context.OpenReadTransaction())
                {
                    var deleteResponse = new PutServerWideBackupConfigurationResponse
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
    }
    
    public class ServerWideBackupConfigurationResults : IDynamicJson
    {
        public List<ServerWideBackupConfigurationForStudio> Results;

        public ServerWideBackupConfigurationResults()
        {
            Results = new List<ServerWideBackupConfigurationForStudio>();
        }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Results)] = new DynamicJsonArray(Results.Select(x => x.ToJson()))
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
