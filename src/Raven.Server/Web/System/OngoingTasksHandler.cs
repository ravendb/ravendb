using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Json.Converters;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.Replication;
using Raven.Server.Web.Studio;
using Sparrow.Extensions;
using Voron.Util.Settings;

namespace Raven.Server.Web.System
{
    public class OngoingTasksHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/backup/database", "OPTIONS", AuthorizationStatus.DatabaseAdmin)]
        public Task AllowPreflightRequest()
        {
            SetupCORSHeaders();
            HttpContext.Response.Headers.Remove("Content-Type");
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/tasks", "GET", AuthorizationStatus.ValidUser)]
        public Task GetOngoingTasks()
        {
            var result = GetOngoingTasksInternal();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }

            return Task.CompletedTask;
        }

        public OngoingTasksResult GetOngoingTasksInternal()
        {
            var ongoingTasksResult = new OngoingTasksResult();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseTopology dbTopology;
                ClusterTopology clusterTopology;
                DatabaseRecord databaseRecord;

                using (context.OpenReadTransaction())
                {
                    databaseRecord = ServerStore.Cluster.ReadDatabase(context, Database.Name);

                    if (databaseRecord == null)
                    {
                        return ongoingTasksResult;
                    }

                    dbTopology = databaseRecord.Topology;
                    clusterTopology = ServerStore.GetClusterTopology(context);
                    ongoingTasksResult.OngoingTasksList.AddRange(CollectSubscriptionTasks(context, databaseRecord, clusterTopology));
                }

                foreach (var tasks in new[]
                {
                    CollectPullReplicationAsHubTasks(clusterTopology),
                    CollectPullReplicationAsSinkTasks(databaseRecord.SinkPullReplications, dbTopology, clusterTopology, databaseRecord.RavenConnectionStrings),
                    CollectExternalReplicationTasks(databaseRecord.ExternalReplications, dbTopology, clusterTopology, databaseRecord.RavenConnectionStrings),
                    CollectEtlTasks(databaseRecord, dbTopology, clusterTopology),
                    CollectBackupTasks(databaseRecord, dbTopology, clusterTopology)
                })
                {
                    ongoingTasksResult.OngoingTasksList.AddRange(tasks);
                }

                ongoingTasksResult.SubscriptionsCount = (int)Database.SubscriptionStorage.GetAllSubscriptionsCount();

                ongoingTasksResult.PullReplication = databaseRecord.HubPullReplications.ToList();

                return ongoingTasksResult;
            }
        }

        private IEnumerable<OngoingTask> CollectPullReplicationAsSinkTasks(List<PullReplicationAsSink> edgePullReplications, DatabaseTopology dbTopology, ClusterTopology clusterTopology, Dictionary<string, RavenConnectionString> connectionStrings)
        {
            if (dbTopology == null)
                yield break;

            var handlers = Database.ReplicationLoader.IncomingHandlers.ToList();
            foreach (var edgeReplication in edgePullReplications)
            {
                yield return GetSinkTaskInfo(dbTopology, clusterTopology, connectionStrings, edgeReplication, handlers);
            }
        }

        private OngoingTaskPullReplicationAsSink GetSinkTaskInfo(
            DatabaseTopology dbTopology, 
            ClusterTopology clusterTopology, 
            Dictionary<string, RavenConnectionString> connectionStrings,
            PullReplicationAsSink sinkReplication, 
            List<IncomingReplicationHandler> handlers)
        {
            var tag = Database.WhoseTaskIsIt(dbTopology, sinkReplication, null);

            (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.NotActive);
            IncomingReplicationHandler handler = null;
            if (tag == ServerStore.NodeTag)
            {
                foreach (var incoming in handlers)
                {
                    if (incoming.PullReplicationName == sinkReplication.HubDefinitionName)
                    {
                        handler = incoming;
                        res = (incoming.ConnectionInfo.SourceUrl, OngoingTaskConnectionStatus.Active);
                        break;
                    }
                }
            }
            else
            {
                res.Status = OngoingTaskConnectionStatus.NotOnThisNode;
            }
            

            var sinkInfo = new OngoingTaskPullReplicationAsSink
            {
                TaskId = sinkReplication.TaskId,
                TaskName = sinkReplication.Name,
                ResponsibleNode = new NodeId {NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag)},
                ConnectionStringName = sinkReplication.ConnectionStringName,
                TaskState = sinkReplication.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = connectionStrings[sinkReplication.ConnectionStringName].Database,
                HubDefinitionName = sinkReplication.HubDefinitionName,
                DestinationUrl = res.Url,
                TopologyDiscoveryUrls = connectionStrings[sinkReplication.ConnectionStringName].TopologyDiscoveryUrls,
                MentorNode = sinkReplication.MentorNode,
                TaskConnectionStatus = res.Status,
            };

            if (sinkReplication.CertificateWithPrivateKey != null)
            {
                // fetch public key of certificate
                var certBytes = Convert.FromBase64String(sinkReplication.CertificateWithPrivateKey);
                var certificate = new X509Certificate2(certBytes, 
                    sinkReplication.CertificatePassword, 
                    X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

                sinkInfo.CertificatePublicKey = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));
            }

            return sinkInfo;
        }

        private IEnumerable<OngoingTask> CollectPullReplicationAsHubTasks(ClusterTopology clusterTopology)
        {
            var pullReplicationHandlers = Database.ReplicationLoader.OutgoingHandlers.Where(n => n.IsPullReplicationAsHub).ToList();
            foreach (var handler in pullReplicationHandlers)
            {
                var ex = handler.Destination as ExternalReplication;
                if (ex == null) // should not happened
                    continue;

                yield return GetPullReplicationAsHubTaskInfo(clusterTopology, ex);
            }
        }

        private OngoingTaskPullReplicationAsHub GetPullReplicationAsHubTaskInfo(ClusterTopology clusterTopology, ExternalReplication ex)
        {
            var connectionResult = Database.ReplicationLoader.GetExternalReplicationDestination(ex.TaskId);
            var tag = Server.ServerStore.NodeTag; // we can't know about pull replication tasks on other nodes.

            return new OngoingTaskPullReplicationAsHub
            {
                TaskId = ex.TaskId,
                TaskName = ex.Name,
                ResponsibleNode = new NodeId {NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag)},
                TaskState = ex.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = ex.Database,
                DestinationUrl = connectionResult.Url,
                MentorNode = ex.MentorNode,
                TaskConnectionStatus = connectionResult.Status,
                DelayReplicationFor = ex.DelayReplicationFor
            };
        }

        private IEnumerable<OngoingTask> CollectSubscriptionTasks(TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(databaseRecord.DatabaseName)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                var tag = Database.WhoseTaskIsIt(databaseRecord.Topology, subscriptionState, subscriptionState);
                OngoingTaskConnectionStatus connectionStatus;
                if (tag != ServerStore.NodeTag)
                {
                    connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
                }
                else if (Database.SubscriptionStorage.TryGetRunningSubscriptionConnection(subscriptionState.SubscriptionId, out var _))
                {
                    connectionStatus = OngoingTaskConnectionStatus.Active;
                }
                else
                {
                    connectionStatus = OngoingTaskConnectionStatus.NotActive;
                }

                yield return new OngoingTaskSubscription
                {
                    // Supply only needed fields for List View  
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    TaskName = subscriptionState.SubscriptionName,
                    TaskState = subscriptionState.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                    TaskId = subscriptionState.SubscriptionId,
                    Query = subscriptionState.Query,
                    TaskConnectionStatus = connectionStatus
                };
            }
        }

        private IEnumerable<OngoingTask> CollectExternalReplicationTasks(List<ExternalReplication> externalReplications, DatabaseTopology dbTopology, ClusterTopology clusterTopology, Dictionary<string, RavenConnectionString> connectionStrings)
        {
            if (dbTopology == null)
                yield break;

            foreach (var externalReplication in externalReplications)
            {
                var taskInfo = GetExternalReplicationInfo(dbTopology, clusterTopology, externalReplication, connectionStrings);
                yield return taskInfo;
            }
        }

        private OngoingTaskReplication GetExternalReplicationInfo(DatabaseTopology databaseTopology, ClusterTopology clusterTopology,
            ExternalReplication watcher, Dictionary<string, RavenConnectionString> connectionStrings)
        {
            var taskStatus = ReplicationLoader.GetExternalReplicationState(ServerStore, Database.Name, watcher.TaskId);
            var tag = Database.WhoseTaskIsIt(databaseTopology, watcher, taskStatus);

            (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.None);
            if (tag == ServerStore.NodeTag)
            {
                res = Database.ReplicationLoader.GetExternalReplicationDestination(watcher.TaskId);
            }
            else
            {
                res.Status = OngoingTaskConnectionStatus.NotOnThisNode;
            }

            var taskInfo = new OngoingTaskReplication
            {
                TaskId = watcher.TaskId,
                TaskName = watcher.Name,
                ResponsibleNode = new NodeId
                {
                    NodeTag = tag,
                    NodeUrl = clusterTopology.GetUrlFromTag(tag)
                },
                ConnectionStringName = watcher.ConnectionStringName,
                TaskState = watcher.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = connectionStrings[watcher.ConnectionStringName].Database,
                DestinationUrl = res.Url,
                TopologyDiscoveryUrls = connectionStrings[watcher.ConnectionStringName].TopologyDiscoveryUrls,
                MentorNode = watcher.MentorNode,
                TaskConnectionStatus = res.Status,
                DelayReplicationFor = watcher.DelayReplicationFor
            };

            return taskInfo;
        }

        [RavenAction("/databases/*/admin/periodic-backup/test-credentials", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task TestPeriodicBackupCredentials()
        {
            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            if (Enum.TryParse(type, out PeriodicBackupTestConnectionType connectionType) == false)
                throw new ArgumentException($"Unknown backup connection: {type}");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DynamicJsonValue result;
                try
                {
                    var connectionInfo = await context.ReadForMemoryAsync(RequestBodyStream(), "test-connection");
                    switch (connectionType)
                    {
                        case PeriodicBackupTestConnectionType.S3:
                            var s3Settings = JsonDeserializationClient.S3Settings(connectionInfo);
                            using (var awsClient = new RavenAwsS3Client(
                                s3Settings.AwsAccessKey, s3Settings.AwsSecretKey, s3Settings.AwsRegionName,
                                s3Settings.BucketName, cancellationToken: ServerStore.ServerShutdown))
                            {
                                await awsClient.TestConnection();
                            }
                            break;
                        case PeriodicBackupTestConnectionType.Glacier:
                            var glacierSettings = JsonDeserializationClient.GlacierSettings(connectionInfo);
                            using (var glacierClient = new RavenAwsGlacierClient(
                                glacierSettings.AwsAccessKey, glacierSettings.AwsSecretKey,
                                glacierSettings.AwsRegionName, glacierSettings.VaultName,
                                cancellationToken: ServerStore.ServerShutdown))
                            {
                                await glacierClient.TestConnection();
                            }
                            break;
                        case PeriodicBackupTestConnectionType.Azure:
                            var azureSettings = JsonDeserializationClient.AzureSettings(connectionInfo);
                            using (var azureClient = new RavenAzureClient(
                                azureSettings.AccountName, azureSettings.AccountKey,
                                azureSettings.StorageContainer, cancellationToken: ServerStore.ServerShutdown))
                            {
                                await azureClient.TestConnection();
                            }
                            break;
                        case PeriodicBackupTestConnectionType.FTP:
                            var ftpSettings = JsonDeserializationClient.FtpSettings(connectionInfo);
                            using (var ftpClient = new RavenFtpClient(ftpSettings.Url, ftpSettings.Port, ftpSettings.UserName,
                                ftpSettings.Password, ftpSettings.CertificateAsBase64, ftpSettings.CertificateFileName))
                            {
                                await ftpClient.TestConnection();
                            }
                            break;
                        case PeriodicBackupTestConnectionType.Local:
                        case PeriodicBackupTestConnectionType.None:
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = true,
                    };
                }
                catch (Exception e)
                {
                    result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = false,
                        [nameof(NodeConnectionTestResult.Error)] = e.ToString()
                    };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }
        }

        [RavenAction("/databases/*/admin/periodic-backup/config", "GET", AuthorizationStatus.DatabaseAdmin)]
        public Task GetConfiguration()
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

        [RavenAction("/databases/*/admin/periodic-backup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdatePeriodicBackup()
        {
            await DatabaseConfigurations(ServerStore.ModifyPeriodicBackup,
                "update-periodic-backup",
                beforeSetupConfiguration: BeforeSetupConfiguration,
                fillJson: (json, readerObject, index) =>
                {
                    var taskIdName = nameof(PeriodicBackupConfiguration.TaskId);
                    readerObject.TryGet(taskIdName, out long taskId);
                    if (taskId == 0)
                        taskId = index;
                    json[taskIdName] = taskId;
                });
        }

        private void BeforeSetupConfiguration(string _, ref BlittableJsonReaderObject readerObject, JsonOperationContext context)
        {
            ServerStore.LicenseManager.AssertCanAddPeriodicBackup(readerObject);
            VerifyPeriodicBackupConfiguration(ref readerObject, context);
        }

        private void AssertDestinationAndRegionAreAllowed(BlittableJsonReaderObject readerObject)
        {
            var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(readerObject);

            foreach (var backupDestination in configuration.GetDestinations())
            {
                ServerStore.Configuration.Backup.AssertDestinationAllowed(backupDestination);
            }

            if (configuration.S3Settings != null && configuration.S3Settings.Disabled == false)
                ServerStore.Configuration.Backup.AssertRegionAllowed(configuration.S3Settings.AwsRegionName);

            if (configuration.GlacierSettings != null && configuration.GlacierSettings.Disabled == false)
                ServerStore.Configuration.Backup.AssertRegionAllowed(configuration.GlacierSettings.AwsRegionName);
        }

        private void VerifyPeriodicBackupConfiguration(ref BlittableJsonReaderObject readerObject, JsonOperationContext context)
        {
            AssertDestinationAndRegionAreAllowed(readerObject);

            readerObject.TryGet(nameof(PeriodicBackupConfiguration.FullBackupFrequency), out string fullBackupFrequency);
            readerObject.TryGet(nameof(PeriodicBackupConfiguration.IncrementalBackupFrequency), out string incrementalBackupFrequency);

            if (VerifyBackupFrequency(fullBackupFrequency) == null &&
                VerifyBackupFrequency(incrementalBackupFrequency) == null)
            {
                throw new ArgumentException("Couldn't parse the cron expressions for both full and incremental backups. " +
                                            $"full backup cron expression: {fullBackupFrequency}, " +
                                            $"incremental backup cron expression: {incrementalBackupFrequency}");
            }

            readerObject.TryGet(nameof(PeriodicBackupConfiguration.LocalSettings),
                out BlittableJsonReaderObject localSettings);

            if (localSettings == null)
                return;

            localSettings.TryGet(nameof(LocalSettings.Disabled), out bool disabled);
            if (disabled)
                return;

            localSettings.TryGet(nameof(LocalSettings.FolderPath), out string folderPath);

            var pathResult = GetActualFullPath(folderPath);
            if (pathResult.Error != null)
                throw new ArgumentException(pathResult.Error);

            folderPath = pathResult.FolderPath;

            if (pathResult.HasLocalRootPath)
            {
                readerObject.Modifications = new DynamicJsonValue
                {
                    [nameof(LocalSettings)] = new DynamicJsonValue
                    {
                        [nameof(LocalSettings.Disabled)] = disabled,
                        [nameof(LocalSettings.FolderPath)] = folderPath
                    }
                };

                readerObject = context.ReadObject(readerObject, "modified-backup-configuration");
            }

            if (DataDirectoryInfo.CanAccessPath(folderPath, out var error) == false)
                throw new ArgumentException(error);
        }

        [RavenAction("/databases/*/admin/backup-data-directory", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task FullBackupDataDirectory()
        {
            var path = GetStringQueryString("path", required: true);
            var requestTimeoutInMs = GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;

            var pathResult = GetActualFullPath(path);
            var getNodesInfo = GetBoolValueQueryString("getNodesInfo", required: false) ?? false;
            var info = new DataDirectoryInfo(ServerStore, pathResult.FolderPath, Database.Name, isBackup: true, getNodesInfo, requestTimeoutInMs, ResponseBodyStream());
            await info.UpdateDirectoryResult(databaseName: Database.Name, error: pathResult.Error);
        }

        private ActualPathResult GetActualFullPath(string folderPath)
        {
            var pathResult = new ActualPathResult();
            if (ServerStore.Configuration.Backup.LocalRootPath == null)
            {
                pathResult.FolderPath = folderPath;

                if (string.IsNullOrWhiteSpace(folderPath))
                {
                    pathResult.Error = "Backup directory cannot be null or empty";
                }

                return pathResult;
            }

            // in this case we receive a path relative to the root path
            try
            {
                pathResult.FolderPath = ServerStore.Configuration.Backup.LocalRootPath.Combine(folderPath).FullPath;
            }
            catch
            {
                pathResult.Error = $"Unable to combine the local root path '{ServerStore.Configuration.Backup.LocalRootPath?.FullPath}' " +
                                         $"with the user supplied relative path '{folderPath}'";
                return pathResult;
            }

            if (PathUtil.IsSubDirectory(pathResult.FolderPath, ServerStore.Configuration.Backup.LocalRootPath.FullPath) == false)
            {
                pathResult.Error = $"The administrator has restricted local backups to be saved under the following root path '{ServerStore.Configuration.Backup.LocalRootPath?.FullPath}' " +
                                         $"but the actual chosen path is '{pathResult.FolderPath}' which is not a sub-directory of the root path.";
                return pathResult;
            }

            pathResult.HasLocalRootPath = true;
            return pathResult;
        }

        private class ActualPathResult
        {
            public bool HasLocalRootPath { get; set; }

            public string FolderPath { get; set; }

            public string Error { get; set; }
        }

        private static CrontabSchedule VerifyBackupFrequency(string backupFrequency)
        {
            if (string.IsNullOrWhiteSpace(backupFrequency))
                return null;

            return CrontabSchedule.Parse(backupFrequency);
        }

        [RavenAction("/databases/*/admin/backup/database", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task BackupDatabase()
        {
            SetupCORSHeaders();

            var taskId = GetLongQueryString("taskId");
            var isFullBackup = GetBoolValueQueryString("isFullBackup", required: false);

            var nodeTag = Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
            if (nodeTag == null)
                throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");

            if (nodeTag == ServerStore.NodeTag)
            {
                var operationId = Database.PeriodicBackupRunner.StartBackupTask(taskId, isFullBackup ?? true);
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.ResponsibleNode));
                    writer.WriteString(ServerStore.NodeTag);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.OperationId));
                    writer.WriteInteger(operationId);
                    writer.WriteEndObject();
                }

                return Task.CompletedTask;
            }

            RedirectToRelevantNode(nodeTag);
            return Task.CompletedTask;
        }

        private void RedirectToRelevantNode(string nodeTag)
        {
            ClusterTopology topology;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                topology = ServerStore.GetClusterTopology(context);
            }
            var url = topology.GetUrlFromTag(nodeTag);
            if (url == null)
            {
                throw new InvalidOperationException($"Couldn't find the node url for node tag: {nodeTag}");
            }

            var location = url + HttpContext.Request.Path + HttpContext.Request.QueryString;
            HttpContext.Response.StatusCode = (int)HttpStatusCode.TemporaryRedirect;
            HttpContext.Response.Headers.Remove("Content-Type");
            HttpContext.Response.Headers.Add("Location", location);
        }

        private IEnumerable<OngoingTask> CollectBackupTasks(
            DatabaseRecord databaseRecord,
            DatabaseTopology dbTopology,
            ClusterTopology clusterTopology)
        {
            if (dbTopology == null)
                yield break;

            if (databaseRecord.PeriodicBackups == null)
                yield break;

            if (databaseRecord.PeriodicBackups.Count == 0)
                yield break;

            foreach (var backupConfiguration in databaseRecord.PeriodicBackups)
            {
                yield return GetOngoingTaskBackup(backupConfiguration.TaskId, databaseRecord, backupConfiguration, clusterTopology);
            }
        }

        private OngoingTaskBackup GetOngoingTaskBackup(
            long taskId,
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration backupConfiguration,
            ClusterTopology clusterTopology)
        {
            var backupStatus = Database.PeriodicBackupRunner.GetBackupStatus(taskId);
            var nextBackup = Database.PeriodicBackupRunner.GetNextBackupDetails(databaseRecord, backupConfiguration, backupStatus);
            var onGoingBackup = Database.PeriodicBackupRunner.OnGoingBackup(taskId);
            var backupDestinations = backupConfiguration.GetDestinations();
            var tag = Database.WhoseTaskIsIt(databaseRecord.Topology, backupConfiguration, backupStatus, useLastResponsibleNodeIfNoAvailableNodes: true);

            return new OngoingTaskBackup
            {
                TaskId = backupConfiguration.TaskId,
                BackupType = backupConfiguration.BackupType,
                TaskName = backupConfiguration.Name,
                TaskState = backupConfiguration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                LastFullBackup = backupStatus.LastFullBackup,
                LastIncrementalBackup = backupStatus.LastIncrementalBackup,
                OnGoingBackup = onGoingBackup,
                NextBackup = nextBackup,
                TaskConnectionStatus = backupConfiguration.Disabled
                    ? OngoingTaskConnectionStatus.NotActive
                    : tag == ServerStore.NodeTag
                        ? OngoingTaskConnectionStatus.Active
                        : OngoingTaskConnectionStatus.NotOnThisNode,
                ResponsibleNode = new NodeId
                {
                    NodeTag = tag,
                    NodeUrl = clusterTopology.GetUrlFromTag(tag)
                },
                BackupDestinations = backupDestinations
            };
        }

        [RavenAction("/databases/*/admin/connection-strings", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task RemoveConnectionString()
        {
            if (TryGetAllowedDbs(Database.Name, out var _, requireAdmin: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var connectionStringName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            ServerStore.EnsureNotPassive();

            var (index, _) = await ServerStore.RemoveConnectionString(Database.Name, connectionStringName, type);
            await ServerStore.Cluster.WaitForIndexNotification(index);
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index
                    });
                    writer.Flush();
                }
            }
        }

        [RavenAction("/databases/*/admin/connection-strings", "GET", AuthorizationStatus.DatabaseAdmin)]
        public Task GetConnectionStrings()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (TryGetAllowedDbs(Database.Name, out var allowedDbs, true) == false)
                return Task.CompletedTask;

            var connectionStringName = GetStringQueryString("connectionStringName", false);
            var type = GetStringQueryString("type", false);

            ServerStore.EnsureNotPassive();
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                DatabaseRecord record;
                using (context.OpenReadTransaction())
                {
                    record = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                }

                Dictionary<string, RavenConnectionString> ravenConnectionStrings;
                Dictionary<string, SqlConnectionString> sqlConnectionStrings;
                if (connectionStringName != null)
                {
                    if (string.IsNullOrWhiteSpace(connectionStringName))
                        throw new ArgumentException($"connectionStringName {connectionStringName}' must have a non empty value");


                    if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                        throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

                    (ravenConnectionStrings, sqlConnectionStrings) = GetConnectionString(record, connectionStringName, connectionStringType);
                }
                else
                {
                    ravenConnectionStrings = record.RavenConnectionStrings;
                    sqlConnectionStrings = record.SqlConnectionStrings;
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var result = new GetConnectionStringsResult
                    {
                        RavenConnectionStrings = ravenConnectionStrings,
                        SqlConnectionStrings = sqlConnectionStrings
                    };
                    context.Write(writer, result.ToJson());
                    writer.Flush();
                }
            }

            return Task.CompletedTask;
        }

        private static (Dictionary<string, RavenConnectionString>, Dictionary<string, SqlConnectionString>)
            GetConnectionString(DatabaseRecord record, string connectionStringName, ConnectionStringType connectionStringType)
        {
            var ravenConnectionStrings = new Dictionary<string, RavenConnectionString>();
            var sqlConnectionStrings = new Dictionary<string, SqlConnectionString>();

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    if (record.RavenConnectionStrings.TryGetValue(connectionStringName, out var ravenConnectionString))
                    {
                        ravenConnectionStrings.TryAdd(connectionStringName, ravenConnectionString);
                    }

                    break;

                case ConnectionStringType.Sql:
                    if (record.SqlConnectionStrings.TryGetValue(connectionStringName, out var sqlConnectionString))
                    {
                        sqlConnectionStrings.TryAdd(connectionStringName, sqlConnectionString);
                    }

                    break;

                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
            }

            return (ravenConnectionStrings, sqlConnectionStrings);
        }

        [RavenAction("/databases/*/admin/connection-strings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutConnectionString()
        {
            await DatabaseConfigurations((_, databaseName, connectionString) => ServerStore.PutConnectionString(_, databaseName, connectionString), "put-connection-string");
        }

        [RavenAction("/databases/*/admin/etl", "RESET", AuthorizationStatus.Operator)]
        public async Task ResetEtl()
        {
            var configurationName = GetStringQueryString("configurationName"); // etl task name
            var transformationName = GetStringQueryString("transformationName");

            await DatabaseConfigurations((_, databaseName, etlConfiguration) => ServerStore.RemoveEtlProcessState(_, databaseName, configurationName, transformationName), "etl-reset");
        }

        [RavenAction("/databases/*/admin/etl", "PUT", AuthorizationStatus.Operator)]
        public async Task AddEtl()
        {
            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, databaseName, etlConfiguration) => ServerStore.AddEtl(_, databaseName, etlConfiguration), "etl-add",
                    beforeSetupConfiguration: AssertCanAddOrUpdateEtl, fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            string etlConfigurationName = null;

            await DatabaseConfigurations((_, databaseName, etlConfiguration) =>
            {
                var task = ServerStore.UpdateEtl(_, databaseName, id.Value, etlConfiguration);
                etlConfiguration.TryGet(nameof(RavenEtlConfiguration.Name), out etlConfigurationName);
                return task;

            }, "etl-update", fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);


            // Reset scripts if needed
            var scriptsToReset = HttpContext.Request.Query["reset"];
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var script in scriptsToReset)
                {
                    await ServerStore.RemoveEtlProcessState(ctx, Database.Name, etlConfigurationName, script);
                }
            }
        }

        private void AssertCanAddOrUpdateEtl(string databaseName, ref BlittableJsonReaderObject etlConfiguration, JsonOperationContext context)
        {
            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    ServerStore.LicenseManager.AssertCanAddRavenEtl();
                    break;
                case EtlType.Sql:
                    ServerStore.LicenseManager.AssertCanAddSqlEtl();
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }
        }

        private IEnumerable<OngoingTask> CollectEtlTasks(DatabaseRecord databaseRecord, DatabaseTopology dbTopology, ClusterTopology clusterTopology)
        {
            if (dbTopology == null)
                yield break;

            if (databaseRecord.RavenEtls != null)
            {
                foreach (var ravenEtl in databaseRecord.RavenEtls)
                {

                    var taskState = GetEtlTaskState(ravenEtl);

                    if (databaseRecord.RavenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var connection) == false)
                        throw new InvalidOperationException(
                            $"Could not find connection string named '{ravenEtl.ConnectionStringName}' in the database record for '{ravenEtl.Name}' ETL");

                    var process = Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, ravenEtl, out var tag, out var error);

                    yield return new OngoingTaskRavenEtlListView()
                    {
                        TaskId = ravenEtl.TaskId,
                        TaskName = ravenEtl.Name,
                        TaskState = taskState,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        DestinationUrl = process?.Url,
                        TaskConnectionStatus = connectionStatus,
                        DestinationDatabase = connection.Database,
                        ConnectionStringName = ravenEtl.ConnectionStringName,
                        TopologyDiscoveryUrls = connection.TopologyDiscoveryUrls,
                        Error = error
                    };
                }
            }

            if (databaseRecord.SqlEtls != null)
            {
                foreach (var sqlEtl in databaseRecord.SqlEtls)
                {
                    if (databaseRecord.SqlConnectionStrings.TryGetValue(sqlEtl.ConnectionStringName, out var sqlConnection) == false)
                        throw new InvalidOperationException(
                            $"Could not find connection string named '{sqlEtl.ConnectionStringName}' in the database record for '{sqlEtl.Name}' ETL");

#pragma warning disable 618
                    var (database, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString(sqlConnection.FactoryName ?? sqlEtl.FactoryName, sqlConnection.ConnectionString);
#pragma warning restore 618

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, sqlEtl, out var tag, out var error);

                    var taskState = GetEtlTaskState(sqlEtl);

                    yield return new OngoingTaskSqlEtlListView()
                    {
                        TaskId = sqlEtl.TaskId,
                        TaskName = sqlEtl.Name,
                        TaskConnectionStatus = connectionStatus,
                        TaskState = taskState,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        DestinationServer = server,
                        DestinationDatabase = database,
                        ConnectionStringName = sqlEtl.ConnectionStringName,
                        Error = error
                    };
                }
            }
        }

        private OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
            where T : ConnectionString
        {
            var connectionStatus = OngoingTaskConnectionStatus.None;
            error = null;

            var processState = EtlLoader.GetProcessState(config.Transforms, Database, config.Name);

            tag = Database.WhoseTaskIsIt(record.Topology, config, processState);

            if (tag == ServerStore.NodeTag)
            {
                var process = Database.EtlLoader.Processes.FirstOrDefault(x => x.ConfigurationName == config.Name);

                if (process != null)
                    connectionStatus = process.GetConnectionStatus();
                else
                {
                    if (config.Disabled)
                        connectionStatus = OngoingTaskConnectionStatus.NotActive;
                    else
                        error = $"ETL process '{config.Name}' was not found.";
                }

            }
            else
            {
                connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
            }

            return connectionStatus;
        }

        // Get Info about a specific task - For Edit View in studio - Each task should return its own specific object
        [RavenAction("/databases/*/task", "GET", AuthorizationStatus.ValidUser)]
        public Task GetOngoingTaskInfo()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = GetLongQueryString("key");
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    var record = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                    if (record == null)
                        throw new DatabaseDoesNotExistException(Database.Name);

                    var dbTopology = record.Topology;

                    if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                        throw new ArgumentException($"Unknown task type: {type}", "type");

                    switch (type)
                    {
                        case OngoingTaskType.Replication:

                            var watcher = record.ExternalReplications.Find(x => x.TaskId == key);
                            if (watcher == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }
                            var taskInfo = GetExternalReplicationInfo(dbTopology, clusterTopology, watcher, record.RavenConnectionStrings);

                            WriteResult(context, taskInfo);

                            break;
                        
                        case OngoingTaskType.PullReplicationAsHub:
                            throw new BadRequestException("Getting task info for " + OngoingTaskType.PullReplicationAsHub + " is not supported");

                        case OngoingTaskType.PullReplicationAsSink:
                            var edge = record.SinkPullReplications.Find(x => x.TaskId == key);
                            if (edge == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }
                            var sinkTaskInfo = GetSinkTaskInfo(dbTopology, clusterTopology, record.RavenConnectionStrings, edge, Database.ReplicationLoader.IncomingHandlers.ToList());

                            WriteResult(context, sinkTaskInfo);
                            break;

                        case OngoingTaskType.Backup:

                            var backupConfiguration = record.PeriodicBackups?.Find(x => x.TaskId == key);
                            if (backupConfiguration == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var backupTaskInfo = GetOngoingTaskBackup(key, record, backupConfiguration, clusterTopology);

                            WriteResult(context, backupTaskInfo);
                            break;

                        case OngoingTaskType.SqlEtl:

                            var sqlEtl = record.SqlEtls?.Find(x => x.TaskId == key);
                            if (sqlEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            WriteResult(context, new OngoingTaskSqlEtlDetails
                            {
                                TaskId = sqlEtl.TaskId,
                                TaskName = sqlEtl.Name,
                                Configuration = sqlEtl,
                                TaskState = GetEtlTaskState(sqlEtl),
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, sqlEtl, out var sqlNode, out var sqlEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = sqlNode,
                                    NodeUrl = clusterTopology.GetUrlFromTag(sqlNode)
                                },
                                Error = sqlEtlError
                            });
                            break;

                        case OngoingTaskType.RavenEtl:

                            var ravenEtl = record.RavenEtls?.Find(x => x.TaskId == key);
                            if (ravenEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var process = Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);

                            WriteResult(context, new OngoingTaskRavenEtlDetails
                            {
                                TaskId = ravenEtl.TaskId,
                                TaskName = ravenEtl.Name,
                                Configuration = ravenEtl,
                                TaskState = GetEtlTaskState(ravenEtl),
                                DestinationUrl = process?.Url,
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, ravenEtl, out var node, out var ravenEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = node,
                                    NodeUrl = clusterTopology.GetUrlFromTag(node)
                                },
                                Error = ravenEtlError
                            });
                            break;

                        case OngoingTaskType.Subscription:

                            var nameKey = GetQueryStringValueAndAssertIfSingleAndNotEmpty("taskName");
                            var itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(record.DatabaseName, nameKey);
                            var doc = ServerStore.Cluster.Read(context, itemKey);
                            if (doc == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);
                            var tag = Database.WhoseTaskIsIt(record.Topology, subscriptionState, subscriptionState);

                            var subscriptionStateInfo = new SubscriptionStateWithNodeDetails
                            {
                                Query = subscriptionState.Query,
                                ChangeVectorForNextBatchStartingPoint = subscriptionState.ChangeVectorForNextBatchStartingPoint,
                                SubscriptionId = subscriptionState.SubscriptionId,
                                SubscriptionName = subscriptionState.SubscriptionName,
                                LastBatchAckTime = subscriptionState.LastBatchAckTime,
                                Disabled = subscriptionState.Disabled,
                                LastClientConnectionTime = subscriptionState.LastClientConnectionTime,
                                MentorNode = subscriptionState.MentorNode,
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = tag,
                                    NodeUrl = clusterTopology.GetUrlFromTag(tag)
                                }
                            };

                            // Todo: here we'll need to talk with the running node? TaskConnectionStatus = subscriptionState.Disabled ? OngoingTaskConnectionStatus.NotActive : OngoingTaskConnectionStatus.Active,

                            WriteResult(context, subscriptionStateInfo.ToJson());
                            break;
                       
                        default:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                    }
                }
            }

            return Task.CompletedTask;
        }
        
        [RavenAction("/databases/*/tasks/pull-replication/hub", "GET", AuthorizationStatus.ValidUser)]
        public Task GetHubTasksInfo()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = GetLongQueryString("key");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    var record = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                    if (record == null)
                        throw new DatabaseDoesNotExistException(Database.Name);

                    var hubReplicationDefinition = record.HubPullReplications?.FirstOrDefault(x => x.TaskId == key);

                    if (hubReplicationDefinition == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    }
                            
                    var currentHandlers = Database.ReplicationLoader.OutgoingHandlers.Where(o => o.Destination is ExternalReplication ex && ex.TaskId == key)
                        .Select(x => GetPullReplicationAsHubTaskInfo(clusterTopology, x.Destination as ExternalReplication))
                        .ToList();

                    var response = new PullReplicationDefinitionAndCurrentConnections
                    {
                        Definition = hubReplicationDefinition,
                        OngoingTasks = currentHandlers
                    };
                            
                    WriteResult(context, response.ToJson());
                }
            }

            return Task.CompletedTask;
        }

        private void WriteResult(JsonOperationContext context, IDynamicJson taskInfo)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, taskInfo.ToJson());
                writer.Flush();
            }
        }

        private void WriteResult(JsonOperationContext context, DynamicJsonValue dynamicJsonValue)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, dynamicJsonValue);
                writer.Flush();
            }
        }
        
        [RavenAction("/databases/*/subscription-tasks/state", "POST", AuthorizationStatus.ValidUser)]
        public async Task ToggleSubscriptionTaskState()
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            if (type != OngoingTaskType.Subscription)
                throw new ArgumentException("Only Subscription type can call this method");

            await ToggleTaskState();
        }

        [RavenAction("/databases/*/admin/tasks/state", "POST", AuthorizationStatus.Operator)]
        public async Task ToggleTaskState()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = GetLongQueryString("key");
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var disable = GetBoolValueQueryString("disable") ?? true;
            var taskName = GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var (index, _) = await ServerStore.ToggleTaskState(key, taskName, type, disable, Database.Name);
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = key,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
                    writer.Flush();
                }
            }
        }

        

        [RavenAction("/databases/*/admin/tasks/external-replication", "POST", AuthorizationStatus.Operator)]
        public async Task UpdateExternalReplication()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                ServerStore.LicenseManager.AssertCanAddExternalReplication();

                ExternalReplication watcher = null;
                await DatabaseConfigurations((_, databaseName, blittableJson) => ServerStore.UpdateExternalReplication(databaseName, blittableJson, out watcher), "update_external_replication",
                    fillJson: (json, _, index) =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            var databaseRecord = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                            var taskStatus = ReplicationLoader.GetExternalReplicationState(ServerStore, Database.Name, watcher.TaskId);
                            json[nameof(OngoingTask.ResponsibleNode)] = Database.WhoseTaskIsIt(databaseRecord.Topology, watcher, taskStatus);
                        }

                        json[nameof(ModifyOngoingTaskResult.TaskId)] = watcher.TaskId == 0 ? index : watcher.TaskId;
                    }, statusCode: HttpStatusCode.Created);
            }
        }

        [RavenAction("/databases/*/subscription-tasks", "DELETE", AuthorizationStatus.ValidUser)]
        public async Task DeleteSubscriptionTask()
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            if (type != OngoingTaskType.Subscription)
                throw new ArgumentException("Only Subscription type can call this method");

            await DeleteOngoingTask();
        }

        [RavenAction("/databases/*/admin/tasks", "DELETE", AuthorizationStatus.Operator)]
        public async Task DeleteOngoingTask()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var id = GetLongQueryString("id");
            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var taskName = GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", "type");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                long index;

                var action = new DeleteOngoingTaskAction(id, type, ServerStore, Database, context);
                try
                {
                    (index, _) = await ServerStore.DeleteOngoingTask(id, taskName, type, Database.Name);
                    await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
                }
                finally
                {
                    await action.Complete();
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = id,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
                    writer.Flush();
                }
            }
        }

        private static OngoingTaskState GetEtlTaskState<T>(EtlConfiguration<T> config) where T : ConnectionString
        {
            var taskState = OngoingTaskState.Enabled;

            if (config.Disabled || config.Transforms.All(x => x.Disabled))
                taskState = OngoingTaskState.Disabled;
            else if (config.Transforms.Any(x => x.Disabled))
                taskState = OngoingTaskState.PartiallyEnabled;

            return taskState;
        }

        private class DeleteOngoingTaskAction
        {
            private readonly ServerStore _serverStore;
            private readonly DocumentDatabase _database;
            private readonly TransactionOperationContext _context;
            private readonly (string Name, List<string> Transformations) _deletingEtl;

            public DeleteOngoingTaskAction(long id, OngoingTaskType type, ServerStore serverStore, DocumentDatabase database, TransactionOperationContext context)
            {
                _serverStore = serverStore;
                _database = database;
                _context = context;

                switch (type)
                {
                    case OngoingTaskType.RavenEtl:
                    case OngoingTaskType.SqlEtl:
                        DatabaseRecord record;

                        using (context.Transaction == null ? context.OpenReadTransaction() : null)
                        {
                            record = _serverStore.Cluster.ReadDatabase(context, database.Name);
                        }

                        if (type == OngoingTaskType.RavenEtl)
                        {
                            var ravenEtl = record.RavenEtls?.Find(x => x.TaskId == id);
                            if (ravenEtl != null)
                                _deletingEtl = (ravenEtl.Name, ravenEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                        }
                        else
                        {
                            var sqlEtl = record.SqlEtls?.Find(x => x.TaskId == id);
                            if (sqlEtl != null)
                                _deletingEtl = (sqlEtl.Name, sqlEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                        }
                        break;
                }
            }

            public async Task Complete()
            {
                if (_deletingEtl.Name != null)
                {
                    foreach (var transformation in _deletingEtl.Transformations)
                    {
                        var (index, _) = await _serverStore.RemoveEtlProcessState(_context, _database.Name, _deletingEtl.Name, transformation);
                        await _database.RachisLogIndexNotifications.WaitForIndexNotification(index, _serverStore.Engine.OperationTimeout);
                    }
                }
            }
        }
    }

    public class OngoingTasksResult : IDynamicJson
    {
        public List<OngoingTask> OngoingTasksList { get; set; }
        public int SubscriptionsCount { get; set; }
        
        public List<PullReplicationDefinition> PullReplication { get; set; }

        public OngoingTasksResult()
        {
            OngoingTasksList = new List<OngoingTask>();
            PullReplication = new List<PullReplicationDefinition>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OngoingTasksList)] = new DynamicJsonArray(OngoingTasksList.Select(x => x.ToJson())),
                [nameof(SubscriptionsCount)] = SubscriptionsCount,
                [nameof(PullReplication)] = new DynamicJsonArray(PullReplication.Select(x => x.ToJson()))
            };
        }
    }
}
