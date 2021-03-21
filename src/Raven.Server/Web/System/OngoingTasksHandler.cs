using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Web.System
{
    public class OngoingTasksHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/tasks", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetOngoingTasks()
        {
            var result = GetOngoingTasksInternal();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
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

                ongoingTasksResult.PullReplications = databaseRecord.HubPullReplications.ToList();

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
                    if (incoming.PullReplicationName == sinkReplication.HubName)
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

            connectionStrings.TryGetValue(sinkReplication.ConnectionStringName, out var connection);

            var sinkInfo = new OngoingTaskPullReplicationAsSink
            {
                TaskId = sinkReplication.TaskId,
                TaskName = sinkReplication.Name,
                ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
                ConnectionStringName = sinkReplication.ConnectionStringName,
                TaskState = sinkReplication.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = connection?.Database,
                HubName = sinkReplication.HubName,
                Mode = sinkReplication.Mode,
                DestinationUrl = res.Url,
                TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
                MentorNode = sinkReplication.MentorNode,
                TaskConnectionStatus = res.Status,
                AccessName = sinkReplication.AccessName,
                AllowedHubToSinkPaths = sinkReplication.AllowedHubToSinkPaths,
                AllowedSinkToHubPaths = sinkReplication.AllowedSinkToHubPaths
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
                ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
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
                    MentorNode = subscriptionState.MentorNode,
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

            connectionStrings.TryGetValue(watcher.ConnectionStringName, out var connection);

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
                DestinationDatabase = connection?.Database,
                DestinationUrl = res.Url,
                TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
                MentorNode = watcher.MentorNode,
                TaskConnectionStatus = res.Status,
                DelayReplicationFor = watcher.DelayReplicationFor
            };

            return taskInfo;
        }

        [RavenAction("/databases/*/admin/periodic-backup/config", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetConfiguration()
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

        [RavenAction("/databases/*/admin/debug/periodic-backup/timers", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetPeriodicBackupTimer()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                BackupDatabaseHandler.WriteStartOfTimers(writer);
                BackupDatabaseHandler.WritePeriodicBackups(Database, writer, context, out int count);
                BackupDatabaseHandler.WriteEndOfTimers(writer, count);
            }
        }

        [RavenAction("/databases/*/admin/periodic-backup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdatePeriodicBackup()
        {
            await DatabaseConfigurations(ServerStore.ModifyPeriodicBackup,
                "update-periodic-backup",
                GetRaftRequestIdFromQuery(),
                beforeSetupConfiguration: (string databaseName, ref BlittableJsonReaderObject readerObject, JsonOperationContext context) =>
                {
                    var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(readerObject);

                    ServerStore.LicenseManager.AssertCanAddPeriodicBackup(configuration);
                    BackupConfigurationHelper.UpdateLocalPathIfNeeded(configuration, ServerStore);
                    BackupConfigurationHelper.AssertBackupConfiguration(configuration);
                    BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(configuration, ServerStore);

                    readerObject = context.ReadObject(configuration.ToJson(), "updated-backup-configuration");
                },
                fillJson: (json, readerObject, index) =>
                {
                    var taskIdName = nameof(PeriodicBackupConfiguration.TaskId);
                    readerObject.TryGet(taskIdName, out long taskId);
                    if (taskId == 0)
                        taskId = index;
                    json[taskIdName] = taskId;
                });
        }

        [RavenAction("/databases/*/admin/backup-data-directory", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task FullBackupDataDirectory()
        {
            var path = GetStringQueryString("path", required: true);
            var requestTimeoutInMs = GetIntValueQueryString("requestTimeoutInMs", required: false) ?? 5 * 1000;
            var getNodesInfo = GetBoolValueQueryString("getNodesInfo", required: false) ?? false;

            var pathSetting = new PathSetting(path);
            await BackupConfigurationHelper.GetFullBackupDataDirectory(pathSetting, Database.Name, requestTimeoutInMs, getNodesInfo, ServerStore, ResponseBodyStream());
        }

        [RavenAction("/databases/*/admin/backup/database", "POST", AuthorizationStatus.DatabaseAdmin, CorsMode = CorsMode.Cluster)]
        public async Task BackupDatabase()
        {
            var taskId = GetLongQueryString("taskId");
            var isFullBackup = GetBoolValueQueryString("isFullBackup", required: false);

            // task id == raft index
            // we must wait here to ensure that the task was actually created on this node
            await ServerStore.Cluster.WaitForIndexNotification(taskId);

            var nodeTag = Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
            if (nodeTag == null)
                throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");

            if (nodeTag == ServerStore.NodeTag)
            {
                var operationId = Database.PeriodicBackupRunner.StartBackupTask(taskId, isFullBackup ?? true);
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.ResponsibleNode));
                    writer.WriteString(ServerStore.NodeTag);
                    writer.WriteComma();
                    writer.WritePropertyName(nameof(StartBackupOperationResult.OperationId));
                    writer.WriteInteger(operationId);
                    writer.WriteEndObject();
                }

                return;
            }

            RedirectToRelevantNode(nodeTag);
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

        private static int _oneTimeBackupCounter;

        [RavenAction("/databases/*/admin/backup", "POST", AuthorizationStatus.DatabaseAdmin, CorsMode = CorsMode.Cluster)]
        public async Task BackupDatabaseOnce()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "database-backup");
                var backupConfiguration = JsonDeserializationServer.BackupConfiguration(json);
                var backupName = $"One Time Backup #{Interlocked.Increment(ref _oneTimeBackupCounter)}";

                PeriodicBackupRunner.CheckServerHealthBeforeBackup(ServerStore, backupName);
                ServerStore.LicenseManager.AssertCanAddPeriodicBackup(backupConfiguration);
                BackupConfigurationHelper.AssertBackupConfigurationInternal(backupConfiguration);
                BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(backupConfiguration, ServerStore);

                var sw = Stopwatch.StartNew();
                ServerStore.ConcurrentBackupsCounter.StartBackup(backupName, Logger);
                try
                {
                    var operationId = ServerStore.Operations.GetNextOperationId();
                    var cancelToken = CreateOperationToken();
                    var backupParameters = new BackupParameters
                    {
                        RetentionPolicy = null,
                        StartTimeUtc = SystemTime.UtcNow,
                        IsOneTimeBackup = true,
                        BackupStatus = new PeriodicBackupStatus { TaskId = -1 },
                        OperationId = operationId,
                        BackupToLocalFolder = BackupConfiguration.CanBackupUsing(backupConfiguration.LocalSettings),
                        IsFullBackup = true,
                        TempBackupPath = (Database.Configuration.Storage.TempPath ?? Database.Configuration.Core.DataDirectory).Combine("OneTimeBackupTemp"),
                        Name = backupName
                    };

                    var backupTask = new BackupTask(Database, backupParameters, backupConfiguration, Logger);

                    var t = Database.Operations.AddOperation(
                        null,
                        $"Manual backup for database: {Database.Name}",
                        Documents.Operations.Operations.OperationType.DatabaseBackup,
                        onProgress =>
                        {
                            var tcs = new TaskCompletionSource<IOperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                            PoolOfThreads.GlobalRavenThreadPool.LongRunning(x =>
                            {
                                try
                                {
                                    Thread.CurrentThread.Priority = ThreadPriority.BelowNormal;
                                    NativeMemory.EnsureRegistered();

                                    using (Database.PreventFromUnloading())
                                    {
                                        var runningBackupStatus = new PeriodicBackupStatus { TaskId = 0, BackupType = backupConfiguration.BackupType };
                                        var backupResult = backupTask.RunPeriodicBackup(onProgress, ref runningBackupStatus);
                                        BackupTask.SaveBackupStatus(runningBackupStatus, Database, Logger, backupResult);
                                        tcs.SetResult(backupResult);
                                    }
                                }
                                catch (OperationCanceledException)
                                {
                                    tcs.SetCanceled();
                                }
                                catch (Exception e)
                                {
                                    if (Logger.IsOperationsEnabled)
                                        Logger.Operations($"Failed to run the backup thread: '{backupName}'", e);

                                    tcs.SetException(e);
                                }
                                finally
                                {
                                    ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);
                                }
                            }, null, $"Backup thread {backupName} for database '{Database.Name}'");
                            return tcs.Task;
                        },
                        id: operationId, token: cancelToken);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                    }
                }
                catch (Exception e)
                {
                    ServerStore.ConcurrentBackupsCounter.FinishBackup(backupName, backupStatus: null, sw.Elapsed, Logger);

                    var message = $"Failed to run backup: '{backupName}'";

                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(message, e);

                    Database.NotificationCenter.Add(AlertRaised.Create(
                        Database.Name,
                        message,
                        null,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));

                    throw;
                }
            }
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
            var responsibleNodeTag = Database.WhoseTaskIsIt(databaseRecord.Topology, backupConfiguration, backupStatus, keepTaskOnOriginalMemberNode: true);
            var nextBackup = Database.PeriodicBackupRunner.GetNextBackupDetails(databaseRecord, backupConfiguration, backupStatus, responsibleNodeTag);
            var onGoingBackup = Database.PeriodicBackupRunner.OnGoingBackup(taskId);
            var backupDestinations = backupConfiguration.GetFullBackupDestinations();

            return new OngoingTaskBackup
            {
                TaskId = backupConfiguration.TaskId,
                BackupType = backupConfiguration.BackupType,
                TaskName = backupConfiguration.Name,
                TaskState = backupConfiguration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                MentorNode = backupConfiguration.MentorNode,
                LastExecutingNodeTag = backupStatus.NodeTag,
                LastFullBackup = backupStatus.LastFullBackup,
                LastIncrementalBackup = backupStatus.LastIncrementalBackup,
                OnGoingBackup = onGoingBackup,
                NextBackup = nextBackup,
                TaskConnectionStatus = backupConfiguration.Disabled
                    ? OngoingTaskConnectionStatus.NotActive
                    : responsibleNodeTag == ServerStore.NodeTag
                        ? OngoingTaskConnectionStatus.Active
                        : OngoingTaskConnectionStatus.NotOnThisNode,
                ResponsibleNode = new NodeId
                {
                    NodeTag = responsibleNodeTag,
                    NodeUrl = clusterTopology.GetUrlFromTag(responsibleNodeTag)
                },
                BackupDestinations = backupDestinations,
                RetentionPolicy = backupConfiguration.RetentionPolicy,
                IsEncrypted = BackupTask.IsBackupEncrypted(Database, backupConfiguration)
            };
        }

        [RavenAction("/databases/*/admin/connection-strings", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task RemoveConnectionString()
        {
            if (await CanAccessDatabaseAsync(Database.Name, requireAdmin: true, requireWrite: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var connectionStringName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("connectionString");
            var type = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            await ServerStore.EnsureNotPassiveAsync();

            var (index, _) = await ServerStore.RemoveConnectionString(Database.Name, connectionStringName, type, GetRaftRequestIdFromQuery());
            await ServerStore.Cluster.WaitForIndexNotification(index);
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index
                    });
                }
            }
        }

        [RavenAction("/databases/*/admin/connection-strings", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetConnectionStrings()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (await CanAccessDatabaseAsync(Database.Name, requireAdmin: true, requireWrite: false) == false)
                return;

            var connectionStringName = GetStringQueryString("connectionStringName", false);
            var type = GetStringQueryString("type", false);

            await ServerStore.EnsureNotPassiveAsync();
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                Dictionary<string, RavenConnectionString> ravenConnectionStrings;
                Dictionary<string, SqlConnectionString> sqlConnectionStrings;

                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    if (connectionStringName != null)
                    {
                        if (string.IsNullOrWhiteSpace(connectionStringName))
                            throw new ArgumentException($"connectionStringName {connectionStringName}' must have a non empty value");

                        if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                            throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");

                        (ravenConnectionStrings, sqlConnectionStrings) = GetConnectionString(rawRecord, connectionStringName, connectionStringType);
                    }
                    else
                    {
                        ravenConnectionStrings = rawRecord.RavenConnectionStrings;
                        sqlConnectionStrings = rawRecord.SqlConnectionStrings;
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var result = new GetConnectionStringsResult
                    {
                        RavenConnectionStrings = ravenConnectionStrings,
                        SqlConnectionStrings = sqlConnectionStrings
                    };
                    context.Write(writer, result.ToJson());
                }
            }
        }

        private static (Dictionary<string, RavenConnectionString>, Dictionary<string, SqlConnectionString>)
            GetConnectionString(RawDatabaseRecord rawRecord, string connectionStringName, ConnectionStringType connectionStringType)
        {
            var ravenConnectionStrings = new Dictionary<string, RavenConnectionString>();
            var sqlConnectionStrings = new Dictionary<string, SqlConnectionString>();

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    var recordRavenConnectionStrings = rawRecord.RavenConnectionStrings;
                    if (recordRavenConnectionStrings != null && recordRavenConnectionStrings.TryGetValue(connectionStringName, out var ravenConnectionString))
                    {
                        ravenConnectionStrings.TryAdd(connectionStringName, ravenConnectionString);
                    }

                    break;

                case ConnectionStringType.Sql:
                    var recordSqlConnectionStrings = rawRecord.SqlConnectionStrings;
                    if (recordSqlConnectionStrings != null && recordSqlConnectionStrings.TryGetValue(connectionStringName, out var sqlConnectionString))
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
            await DatabaseConfigurations((_, databaseName, connectionString, guid) => ServerStore.PutConnectionString(_, databaseName, connectionString, guid), "put-connection-string", GetRaftRequestIdFromQuery());
        }

        [RavenAction("/databases/*/admin/etl", "RESET", AuthorizationStatus.DatabaseAdmin)]
        public async Task ResetEtl()
        {
            var configurationName = GetStringQueryString("configurationName"); // etl task name
            var transformationName = GetStringQueryString("transformationName");

            await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) => ServerStore.RemoveEtlProcessState(_, databaseName, configurationName, transformationName, guid), "etl-reset", GetRaftRequestIdFromQuery());
        }

        [RavenAction("/databases/*/admin/etl", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddEtl()
        {
            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) => ServerStore.AddEtl(_, databaseName, etlConfiguration, guid), "etl-add",
                    GetRaftRequestIdFromQuery(), beforeSetupConfiguration: AssertCanAddOrUpdateEtl, fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            string etlConfigurationName = null;

            await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) =>
            {
                var task = ServerStore.UpdateEtl(_, databaseName, id.Value, etlConfiguration, guid);
                etlConfiguration.TryGet(nameof(RavenEtlConfiguration.Name), out etlConfigurationName);
                return task;
            }, "etl-update",
                GetRaftRequestIdFromQuery(),
                fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

            // Reset scripts if needed
            var scriptsToReset = HttpContext.Request.Query["reset"];
            var raftRequestId = GetRaftRequestIdFromQuery();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var script in scriptsToReset)
                {
                    await ServerStore.RemoveEtlProcessState(ctx, Database.Name, etlConfigurationName, script, $"{raftRequestId}/{script}");
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

                    databaseRecord.RavenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var connection);

                    var process = Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, ravenEtl, out var tag, out var error);

                    yield return new OngoingTaskRavenEtlListView()
                    {
                        TaskId = ravenEtl.TaskId,
                        TaskName = ravenEtl.Name,
                        TaskState = taskState,
                        MentorNode = ravenEtl.MentorNode,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        DestinationUrl = process?.Url,
                        TaskConnectionStatus = connectionStatus,
                        DestinationDatabase = connection?.Database,
                        ConnectionStringName = ravenEtl.ConnectionStringName,
                        TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
                        Error = error
                    };
                }
            }

            if (databaseRecord.SqlEtls != null)
            {
                foreach (var sqlEtl in databaseRecord.SqlEtls)
                {
                    string database = null;
                    string server = null;

                    if (databaseRecord.SqlConnectionStrings.TryGetValue(sqlEtl.ConnectionStringName, out var sqlConnection))
                    {
                        (database, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString(sqlConnection.FactoryName, sqlConnection.ConnectionString);
                    }

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, sqlEtl, out var tag, out var error);

                    var taskState = GetEtlTaskState(sqlEtl);

                    yield return new OngoingTaskSqlEtlListView
                    {
                        TaskId = sqlEtl.TaskId,
                        TaskName = sqlEtl.Name,
                        TaskConnectionStatus = connectionStatus,
                        TaskState = taskState,
                        MentorNode = sqlEtl.MentorNode,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        DestinationServer = server,
                        DestinationDatabase = database,
                        ConnectionStringDefined = sqlConnection != null,
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
        [RavenAction("/databases/*/task", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetOngoingTaskInfo()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);
            long key = 0;
            var taskId = GetLongQueryString("key", false);
            if (taskId != null)
                key = taskId.Value;
            var name = GetStringQueryString("taskName", false);

            if ((taskId == null) && (name == null))
                throw new ArgumentException($"You must specify a query string argument of either 'key' or 'name' , but none was specified.");

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

                            var watcher = name != null ?
                                record.ExternalReplications.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.ExternalReplications.Find(x => x.TaskId == key);

                            if (watcher == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }
                            var taskInfo = GetExternalReplicationInfo(dbTopology, clusterTopology, watcher, record.RavenConnectionStrings);

                            await WriteResult(context, taskInfo);

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

                            await WriteResult(context, sinkTaskInfo);
                            break;

                        case OngoingTaskType.Backup:

                            var backupConfiguration = name != null ?
                                record.PeriodicBackups.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.PeriodicBackups?.Find(x => x.TaskId == key);

                            if (backupConfiguration == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var backupTaskInfo = GetOngoingTaskBackup(key, record, backupConfiguration, clusterTopology);

                            await WriteResult(context, backupTaskInfo);
                            break;

                        case OngoingTaskType.SqlEtl:

                            var sqlEtl = name != null ?
                                record.SqlEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.SqlEtls?.Find(x => x.TaskId == key);

                            if (sqlEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            await WriteResult(context, new OngoingTaskSqlEtlDetails
                            {
                                TaskId = sqlEtl.TaskId,
                                TaskName = sqlEtl.Name,
                                MentorNode = sqlEtl.MentorNode,
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

                            var ravenEtl = name != null ?
                                record.RavenEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.RavenEtls?.Find(x => x.TaskId == key);

                            if (ravenEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var process = Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);

                            await WriteResult(context, new OngoingTaskRavenEtlDetails
                            {
                                TaskId = ravenEtl.TaskId,
                                TaskName = ravenEtl.Name,
                                Configuration = ravenEtl,
                                TaskState = GetEtlTaskState(ravenEtl),
                                MentorNode = ravenEtl.MentorNode,
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
                            string itemKey;
                            if (name == null)
                            {
                                name = Database.SubscriptionStorage.GetSubscriptionNameById(context, key);
                                if (name == null)
                                    throw new SubscriptionDoesNotExistException($"Subscription with id '{key}' was not found in server store");
                            }
                            itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(record.DatabaseName, name);
                            var doc = ServerStore.Cluster.Read(context, itemKey);
                            if (doc == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);
                            var tag = Database.WhoseTaskIsIt(record.Topology, subscriptionState, subscriptionState);
                            var subscriptionStateInfo = new OngoingTaskSubscription
                            {
                                TaskName = subscriptionState.SubscriptionName,
                                TaskId = subscriptionState.SubscriptionId,
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

                            await WriteResult(context, subscriptionStateInfo);
                            break;

                        default:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                    }
                }
            }
        }

        [RavenAction("/databases/*/tasks/pull-replication/hub", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetHubTasksInfo()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = GetLongQueryString("key");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    PullReplicationDefinition def;
                    using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                    {
                        if (rawRecord == null)
                            throw new DatabaseDoesNotExistException(Database.Name);

                        def = rawRecord.GetHubPullReplicationById(key);
                    }

                    if (def == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;
                    }

                    var currentHandlers = Database.ReplicationLoader.OutgoingHandlers.Where(o => o.Destination is ExternalReplication ex && ex.TaskId == key)
                        .Select(x => GetPullReplicationAsHubTaskInfo(clusterTopology, x.Destination as ExternalReplication))
                        .ToList();

                    var response = new PullReplicationDefinitionAndCurrentConnections
                    {
                        Definition = def,
                        OngoingTasks = currentHandlers
                    };

                    await WriteResult(context, response.ToJson());
                }
            }
        }

        private async Task WriteResult(JsonOperationContext context, IDynamicJson taskInfo)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, taskInfo.ToJson());
            }
        }

        private async Task WriteResult(JsonOperationContext context, DynamicJsonValue dynamicJsonValue)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, dynamicJsonValue);
            }
        }

        [RavenAction("/databases/*/subscription-tasks/state", "POST", AuthorizationStatus.ValidUser, EndpointType.Write)]
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

        [RavenAction("/databases/*/admin/tasks/state", "POST", AuthorizationStatus.DatabaseAdmin)]
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
                var (index, _) = await ServerStore.ToggleTaskState(key, taskName, type, disable, Database.Name, GetRaftRequestIdFromQuery());
                await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = key,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
                }
            }
        }

        [RavenAction("/databases/*/admin/tasks/external-replication", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdateExternalReplication()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                ExternalReplication watcher = null;
                await DatabaseConfigurations(
                    (_, databaseName, blittableJson, guid) => ServerStore.UpdateExternalReplication(databaseName, blittableJson, guid, out watcher), "update_external_replication",
                    GetRaftRequestIdFromQuery(),
                    fillJson: (json, _, index) =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            var topology = ServerStore.Cluster.ReadDatabaseTopology(context, Database.Name);
                            var taskStatus = ReplicationLoader.GetExternalReplicationState(ServerStore, Database.Name, watcher.TaskId);
                            json[nameof(OngoingTask.ResponsibleNode)] = Database.WhoseTaskIsIt(topology, watcher, taskStatus);
                        }

                        json[nameof(ModifyOngoingTaskResult.TaskId)] = watcher.TaskId == 0 ? index : watcher.TaskId;
                    }, statusCode: HttpStatusCode.Created);
            }
        }

        [RavenAction("/databases/*/subscription-tasks", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
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

        [RavenAction("/databases/*/admin/tasks", "DELETE", AuthorizationStatus.DatabaseAdmin)]
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
                var raftRequestId = GetRaftRequestIdFromQuery();

                try
                {
                    (index, _) = await ServerStore.DeleteOngoingTask(id, taskName, type, Database.Name, $"{raftRequestId}/delete-ongoing-task");
                    await Database.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);

                    if (type == OngoingTaskType.Subscription)
                    {
                        Database.SubscriptionStorage.RaiseNotificationForTaskRemoved(taskName);
                    }
                }
                finally
                {
                    await action.Complete($"{raftRequestId}/complete");
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = id,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
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
                        using (context.Transaction == null ? context.OpenReadTransaction() : null)
                        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, database.Name))
                        {
                            if (rawRecord == null)
                                break;

                            if (type == OngoingTaskType.RavenEtl)
                            {
                                var ravenEtls = rawRecord.RavenEtls;
                                var ravenEtl = ravenEtls?.Find(x => x.TaskId == id);
                                if (ravenEtl != null)
                                    _deletingEtl = (ravenEtl.Name, ravenEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            }
                            else
                            {
                                var sqlEtls = rawRecord.SqlEtls;
                                var sqlEtl = sqlEtls?.Find(x => x.TaskId == id);
                                if (sqlEtl != null)
                                    _deletingEtl = (sqlEtl.Name, sqlEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            }
                        }
                        break;
                }
            }

            public async Task Complete(string raftRequestId)
            {
                if (_deletingEtl.Name != null)
                {
                    foreach (var transformation in _deletingEtl.Transformations)
                    {
                        var (index, _) = await _serverStore.RemoveEtlProcessState(_context, _database.Name, _deletingEtl.Name, transformation,
                            $"{raftRequestId}/{transformation}");
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

        public List<PullReplicationDefinition> PullReplications { get; set; }

        public OngoingTasksResult()
        {
            OngoingTasksList = new List<OngoingTask>();
            PullReplications = new List<PullReplicationDefinition>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OngoingTasksList)] = new DynamicJsonArray(OngoingTasksList.Select(x => x.ToJson())),
                [nameof(SubscriptionsCount)] = SubscriptionsCount,
                [nameof(PullReplications)] = new DynamicJsonArray(PullReplications.Select(x => x.ToJson()))
            };
        }
    }
}
