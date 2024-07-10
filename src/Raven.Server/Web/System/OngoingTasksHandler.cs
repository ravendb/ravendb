using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
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
using Sparrow.Logging;
using Sparrow.Server.Utils;
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
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
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
            connectionStrings.TryGetValue(sinkReplication.ConnectionStringName, out var connection);
            sinkReplication.Database = connection?.Database;
            sinkReplication.ConnectionString = connection;

            var tag = OngoingTasksUtils.WhoseTaskIsIt(ServerStore, dbTopology, sinkReplication, null, Database.NotificationCenter);

            (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.NotActive);
            IncomingReplicationHandler handler = null;
            if (tag == ServerStore.NodeTag)
            {
                foreach (var incoming in handlers)
                {
                    if (incoming._incomingPullReplicationParams?.Name == sinkReplication.HubName)
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
                ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
                ConnectionStringName = sinkReplication.ConnectionStringName,
                TaskState = sinkReplication.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = connection?.Database,
                HubName = sinkReplication.HubName,
                Mode = sinkReplication.Mode,
                DestinationUrl = res.Url,
                TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
                MentorNode = sinkReplication.MentorNode,
                PinToMentorNode = sinkReplication.PinToMentorNode,
                TaskConnectionStatus = res.Status,
                AccessName = sinkReplication.AccessName,
                AllowedHubToSinkPaths = sinkReplication.AllowedHubToSinkPaths,
                AllowedSinkToHubPaths = sinkReplication.AllowedSinkToHubPaths
            };

            if (sinkReplication.CertificateWithPrivateKey != null)
            {
                // fetch public key of certificate
                var certBytes = Convert.FromBase64String(sinkReplication.CertificateWithPrivateKey);
                var certificate = CertificateLoaderUtil.CreateCertificate(certBytes,
                    sinkReplication.CertificatePassword,
                    CertificateLoaderUtil.FlagsForExport);

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
            var connectionResult = Database.ReplicationLoader.GetPullReplicationDestination(ex.TaskId, ex.Database);
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
                PinToMentorNode = ex.PinToMentorNode,
                TaskConnectionStatus = connectionResult.Status,
                DelayReplicationFor = ex.DelayReplicationFor
            };
        }

        private IEnumerable<OngoingTask> CollectSubscriptionTasks(TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology)
        {
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(databaseRecord.DatabaseName)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                var tag = OngoingTasksUtils.WhoseTaskIsIt(ServerStore, databaseRecord.Topology, subscriptionState, subscriptionState, Database.NotificationCenter);
                OngoingTaskConnectionStatus connectionStatus;
                if (tag != ServerStore.NodeTag)
                {
                    connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
                }
                else if (Database.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(subscriptionState.SubscriptionId, out var connectionsState))
                {
                    connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
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
            connectionStrings.TryGetValue(watcher.ConnectionStringName, out var connection);
            watcher.Database = connection?.Database;
            watcher.ConnectionString = connection;

            var taskStatus = ReplicationLoader.GetExternalReplicationState(ServerStore, Database.Name, watcher.TaskId);
            var tag = OngoingTasksUtils.WhoseTaskIsIt(ServerStore, databaseTopology, watcher, taskStatus, Database.NotificationCenter);

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
                DestinationDatabase = connection?.Database,
                DestinationUrl = res.Url,
                TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
                MentorNode = watcher.MentorNode,
                PinToMentorNode = watcher.PinToMentorNode,
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

        public const string UpdatePeriodicBackupDebugTag = "update-periodic-backup";

        [RavenAction("/databases/*/admin/periodic-backup", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdatePeriodicBackup()
        {
            await DatabaseConfigurations(ServerStore.ModifyPeriodicBackup,
                UpdatePeriodicBackupDebugTag,
                GetRaftRequestIdFromQuery(),
                beforeSetupConfiguration: (string databaseName, ref BlittableJsonReaderObject readerObject, JsonOperationContext context) =>
                {
                    var configuration = JsonDeserializationCluster.PeriodicBackupConfiguration(readerObject);
                    var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

                    ServerStore.LicenseManager.AssertCanAddPeriodicBackup(configuration);
                    BackupConfigurationHelper.UpdateLocalPathIfNeeded(configuration, ServerStore);
                    BackupConfigurationHelper.AssertBackupConfiguration(configuration, Database.Configuration.Backup);
                    BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(configuration, ServerStore);
                    SecurityClearanceValidator.AssertSecurityClearance(configuration, feature?.Status);

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
            {
                // this can happen if the database was just created or if a new task that was just created
                // we'll wait for the cluster observer to give more time for the database stats to become stable,
                // and then we'll wait for the cluster observer to determine the responsible node for the backup

                var task = Task.Delay(Database.Configuration.Cluster.StabilizationTime.AsTimeSpan + Database.Configuration.Cluster.StabilizationTime.AsTimeSpan);
                
                while (true)
                {
                    if (Task.WaitAny(new[] { task }, millisecondsTimeout: 100) == 0)
                    {
                        throw new InvalidOperationException($"Couldn't find a node which is responsible for backup task id: {taskId}");
                    }

                    nodeTag = Database.PeriodicBackupRunner.WhoseTaskIsIt(taskId);
                    if (nodeTag != null)
                        break;
                }
            }

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
            HttpContext.Response.Headers["Location"] = location;
        }

        private static int _oneTimeBackupCounter;
        public const string BackupDatabaseOnceTag = "one-time-database-backup";

        [RavenAction("/databases/*/admin/backup", "POST", AuthorizationStatus.DatabaseAdmin, CorsMode = CorsMode.Cluster)]
        public async Task BackupDatabaseOnce()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "database-backup");
                var backupConfiguration = JsonDeserializationServer.BackupConfiguration(json);
                var backupName = $"One Time Backup #{Interlocked.Increment(ref _oneTimeBackupCounter)}";
                var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

                BackupUtils.CheckServerHealthBeforeBackup(ServerStore, backupName);
                ServerStore.LicenseManager.AssertCanAddPeriodicBackup(backupConfiguration);
                BackupConfigurationHelper.AssertBackupConfigurationInternal(backupConfiguration);
                BackupConfigurationHelper.AssertDestinationAndRegionAreAllowed(backupConfiguration, ServerStore);
                SecurityClearanceValidator.AssertSecurityClearance(backupConfiguration, feature?.Status);

                var sw = Stopwatch.StartNew();
                ServerStore.ConcurrentBackupsCounter.StartBackup(backupName, Logger);
                try
                {
                    var operationId = ServerStore.Operations.GetNextOperationId();
                    var backupParameters = new BackupParameters
                    {
                        RetentionPolicy = null,
                        StartTimeUtc = SystemTime.UtcNow,
                        IsOneTimeBackup = true,
                        BackupStatus = new PeriodicBackupStatus { TaskId = -1 },
                        OperationId = operationId,
                        BackupToLocalFolder = BackupConfiguration.CanBackupUsing(backupConfiguration.LocalSettings),
                        IsFullBackup = true,
                        TempBackupPath = BackupUtils.GetBackupTempPath(Database.Configuration, "OneTimeBackupTemp", out _),
                        Name = backupName
                    };

                    var backupTask = BackupUtils.GetBackupTask(Database, backupParameters, backupConfiguration, Logger, Database.PeriodicBackupRunner._forTestingPurposes);
                    var cancelToken = backupTask.TaskCancelToken;

                    var threadName = $"Backup thread {backupName} for database '{Database.Name}'";

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
                                    ThreadHelper.TrySetThreadPriority(ThreadPriority.BelowNormal, threadName, Logger);
                                    NativeMemory.EnsureRegistered();

                                    using (Database.PreventFromUnloadingByIdleOperations())
                                    {
                                        var runningBackupStatus = new PeriodicBackupStatus { TaskId = 0, BackupType = backupConfiguration.BackupType };
                                        var backupResult = backupTask.RunPeriodicBackup(onProgress, ref runningBackupStatus);
                                        BackupUtils.SaveBackupStatus(runningBackupStatus, Database.Name, Database.ServerStore, Logger, backupResult, operationCancelToken: cancelToken);
                                        tcs.SetResult(backupResult);
                                    }
                                }
                                catch (Exception e) when (e.ExtractSingleInnerException() is OperationCanceledException oce)
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
                            }, null, ThreadNames.ForBackup(threadName, backupName, Database.Name));
                            return tcs.Task;
                        },
                        id: operationId, token: cancelToken);

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                    }

                    LogTaskToAudit(BackupDatabaseOnceTag, operationId, json);
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
                yield return GetOngoingTaskBackup(backupConfiguration.TaskId, backupConfiguration, clusterTopology);
            }
        }

        private OngoingTaskBackup GetOngoingTaskBackup(long taskId, PeriodicBackupConfiguration backupConfiguration, ClusterTopology clusterTopology)
        {
            var backupStatus = Database.PeriodicBackupRunner.GetBackupStatus(taskId);
            var nextBackup = Database.PeriodicBackupRunner.GetNextBackupDetails(backupConfiguration, backupStatus, out var responsibleNodeTag);
            var onGoingBackup = Database.PeriodicBackupRunner.OnGoingBackup(taskId);
            var backupDestinations = backupConfiguration.GetFullBackupDestinations();

            return new OngoingTaskBackup
            {
                TaskId = backupConfiguration.TaskId,
                BackupType = backupConfiguration.BackupType,
                TaskName = backupConfiguration.Name,
                TaskState = backupConfiguration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                MentorNode = backupConfiguration.MentorNode,
                PinToMentorNode = backupConfiguration.PinToMentorNode,
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

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                LogAuditFor(Database.Name, "DELETE", $"Connection string '{connectionStringName}'");
            }

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
                Dictionary<string, OlapConnectionString> olapConnectionStrings;
                Dictionary<string, ElasticSearchConnectionString> elasticSearchConnectionStrings;
                Dictionary<string, QueueConnectionString> queueConnectionStrings;

                using (context.OpenReadTransaction())
                using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    if (connectionStringName != null)
                    {
                        if (string.IsNullOrWhiteSpace(connectionStringName))
                            throw new ArgumentException($"connectionStringName {connectionStringName}' must have a non empty value");

                        if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                            throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");


                        (ravenConnectionStrings, sqlConnectionStrings, olapConnectionStrings, elasticSearchConnectionStrings, queueConnectionStrings) = GetConnectionString(rawRecord, connectionStringName, connectionStringType);
                    }
                    else
                    {
                        ravenConnectionStrings = rawRecord.RavenConnectionStrings;
                        sqlConnectionStrings = rawRecord.SqlConnectionStrings;
                        olapConnectionStrings = rawRecord.OlapConnectionString;
                        elasticSearchConnectionStrings = rawRecord.ElasticSearchConnectionStrings;
                        queueConnectionStrings = rawRecord.QueueConnectionStrings;
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var result = new GetConnectionStringsResult
                    {
                        RavenConnectionStrings = ravenConnectionStrings,
                        SqlConnectionStrings = sqlConnectionStrings,
                        OlapConnectionStrings = olapConnectionStrings,
                        ElasticSearchConnectionStrings = elasticSearchConnectionStrings,
                        QueueConnectionStrings = queueConnectionStrings
                    };
                    context.Write(writer, result.ToJson());
                }
            }
        }

        private static (Dictionary<string, RavenConnectionString>, Dictionary<string, SqlConnectionString>, Dictionary<string, OlapConnectionString>, Dictionary<string, ElasticSearchConnectionString>, Dictionary<string, QueueConnectionString>)
            GetConnectionString(RawDatabaseRecord rawRecord, string connectionStringName, ConnectionStringType connectionStringType)
        {
            var ravenConnectionStrings = new Dictionary<string, RavenConnectionString>();
            var sqlConnectionStrings = new Dictionary<string, SqlConnectionString>();
            var olapConnectionStrings = new Dictionary<string, OlapConnectionString>();
            var elasticSearchConnectionStrings = new Dictionary<string, ElasticSearchConnectionString>();
            var queueConnectionStrings = new Dictionary<string, QueueConnectionString>();

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

                case ConnectionStringType.Olap:
                    var recordOlapConnectionStrings = rawRecord.OlapConnectionString;
                    if (recordOlapConnectionStrings != null && recordOlapConnectionStrings.TryGetValue(connectionStringName, out var olapConnectionString))
                    {
                        olapConnectionStrings.TryAdd(connectionStringName, olapConnectionString);
                    }

                    break;

                case ConnectionStringType.ElasticSearch:
                    var recordElasticConnectionStrings = rawRecord.ElasticSearchConnectionStrings;
                    if (recordElasticConnectionStrings != null && recordElasticConnectionStrings.TryGetValue(connectionStringName, out var elasticConnectionString))
                    {
                        elasticSearchConnectionStrings.TryAdd(connectionStringName, elasticConnectionString);
                    }

                    break;

                case ConnectionStringType.Queue:
                    var recordQueueConnectionStrings = rawRecord.QueueConnectionStrings;
                    if (recordQueueConnectionStrings != null && recordQueueConnectionStrings.TryGetValue(connectionStringName, out var queueConnectionString))
                    {
                        queueConnectionStrings.TryAdd(connectionStringName, queueConnectionString);
                    }

                    break;

                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
            }

            return (ravenConnectionStrings, sqlConnectionStrings, olapConnectionStrings, elasticSearchConnectionStrings, queueConnectionStrings);
        }

        public const string PutConnectionStringDebugTag = "put-connection-string";

        [RavenAction("/databases/*/admin/connection-strings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutConnectionString()
        {
            await DatabaseConfigurations((_, databaseName, connectionString, guid) =>
                {
                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        if (connectionString.TryGet(nameof(ConnectionString.Name), out string name))
                            LogAuditFor(Database.Name, "PUT", $"Connection string '{name}'");
                    }

                    return ServerStore.PutConnectionString(_, databaseName, connectionString, guid);
                },
                PutConnectionStringDebugTag, GetRaftRequestIdFromQuery(),
                beforeSetupConfiguration: (string databaseName, ref BlittableJsonReaderObject readerObject, JsonOperationContext context) =>
                {
                    var connectionStringType = ConnectionString.GetConnectionStringType(readerObject);
                    var connectionString = GetConnectionString(readerObject, connectionStringType);

                    List<string> errors = new();
                    if (connectionString.Validate(ref errors) == false)
                    {
                        throw new BadRequestException($"Invalid connection string configuration. Errors: {string.Join(Environment.NewLine, errors)}");
                    }

                    var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
                    SecurityClearanceValidator.AssertSecurityClearance(connectionString, feature?.Status);
                });
        }

        private static ConnectionString GetConnectionString(BlittableJsonReaderObject readerObject, ConnectionStringType connectionStringType)
        {
            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    return JsonDeserializationCluster.RavenConnectionString(readerObject);
                case ConnectionStringType.Sql:
                    return JsonDeserializationCluster.SqlConnectionString(readerObject);
                case ConnectionStringType.Olap:
                    return JsonDeserializationCluster.OlapConnectionString(readerObject);
                case ConnectionStringType.ElasticSearch:
                    return JsonDeserializationCluster.ElasticSearchConnectionString(readerObject);
                case ConnectionStringType.Queue:
                    return JsonDeserializationCluster.QueueConnectionString(readerObject);
                case ConnectionStringType.None:
                default:
                    throw new ArgumentOutOfRangeException(nameof(connectionStringType), connectionStringType, "Unexpected connection string type.");
            }
        }


        [RavenAction("/databases/*/admin/etl", "RESET", AuthorizationStatus.DatabaseAdmin)]
        public async Task ResetEtl()
        {
            var configurationName = GetStringQueryString("configurationName"); // etl task name
            var transformationName = GetStringQueryString("transformationName");

            await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) => ServerStore.RemoveEtlProcessState(_, databaseName, configurationName, transformationName, guid), "etl-reset", GetRaftRequestIdFromQuery(), statusCode: HttpStatusCode.OK);
        }

        public const string AddEtlDebugTag = "etl-add";

        [RavenAction("/databases/*/admin/etl", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddEtl()
        {
            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) =>
                        ServerStore.AddEtl(_, databaseName, etlConfiguration, guid), AddEtlDebugTag,
                    GetRaftRequestIdFromQuery(),
                    beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                    fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

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
                beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
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
                case EtlType.Olap:
                    ServerStore.LicenseManager.AssertCanAddOlapEtl();
                    break;
                case EtlType.ElasticSearch:
                    ServerStore.LicenseManager.AssertCanAddElasticSearchEtl();
                    break;
                case EtlType.Queue:
                    ServerStore.LicenseManager.AssertCanAddQueueEtl();
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
                        PinToMentorNode = ravenEtl.PinToMentorNode,
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

            if (databaseRecord.OlapEtls != null)
            {
                foreach (var olapEtl in databaseRecord.OlapEtls)
                {
                    string destination = default;
                    if (databaseRecord.OlapConnectionStrings.TryGetValue(olapEtl.ConnectionStringName, out var olapConnection))
                    {
                        destination = olapConnection.GetDestination();
                    }

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, olapEtl, out var tag, out var error);

                    var taskState = GetEtlTaskState(olapEtl);

                    yield return new OngoingTaskOlapEtlListView
                    {
                        TaskId = olapEtl.TaskId,
                        TaskName = olapEtl.Name,
                        TaskConnectionStatus = connectionStatus,
                        TaskState = taskState,
                        MentorNode = olapEtl.MentorNode,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        ConnectionStringName = olapEtl.ConnectionStringName,
                        Destination = destination,
                        Error = error
                    };
                }
            }

            if (databaseRecord.ElasticSearchEtls != null)
            {
                foreach (var elasticSearchEtl in databaseRecord.ElasticSearchEtls)
                {
                    databaseRecord.ElasticSearchConnectionStrings.TryGetValue(elasticSearchEtl.ConnectionStringName, out var connection);

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, elasticSearchEtl, out var tag, out var error);
                    var taskState = GetEtlTaskState(elasticSearchEtl);

                    yield return new OngoingTaskElasticSearchEtlListView
                    {
                        TaskId = elasticSearchEtl.TaskId,
                        TaskName = elasticSearchEtl.Name,
                        TaskConnectionStatus = connectionStatus,
                        TaskState = taskState,
                        MentorNode = elasticSearchEtl.MentorNode,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        ConnectionStringName = elasticSearchEtl.ConnectionStringName,
                        NodesUrls = connection?.Nodes,
                        Error = error
                    };
                }
            }

            if (databaseRecord.QueueEtls != null)
            {
                foreach (var queueEtl in databaseRecord.QueueEtls)
                {
                    databaseRecord.QueueConnectionStrings.TryGetValue(queueEtl.ConnectionStringName, out var connection);

                    var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, queueEtl, out var tag, out var error);
                    var taskState = GetEtlTaskState(queueEtl);

                    yield return new OngoingTaskQueueEtlListView
                    {
                        TaskId = queueEtl.TaskId,
                        TaskName = queueEtl.Name,
                        TaskConnectionStatus = connectionStatus,
                        TaskState = taskState,
                        MentorNode = queueEtl.MentorNode,
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        BrokerType = queueEtl.BrokerType,
                        ConnectionStringName = queueEtl.ConnectionStringName,
                        Url = connection?.GetUrl(),
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

            tag = OngoingTasksUtils.WhoseTaskIsIt(ServerStore, record.Topology, config, processState, Database.NotificationCenter);

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

                            var backupTaskInfo = GetOngoingTaskBackup(key, backupConfiguration, clusterTopology);

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
                                Configuration = sqlEtl,
                                MentorNode = sqlEtl.MentorNode,
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

                        case OngoingTaskType.OlapEtl:

                            var olapEtl = name != null ?
                                record.OlapEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.OlapEtls?.Find(x => x.TaskId == key);

                            if (olapEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            await WriteResult(context, new OngoingTaskOlapEtlDetails
                            {
                                TaskId = olapEtl.TaskId,
                                TaskName = olapEtl.Name,
                                MentorNode = olapEtl.MentorNode,
                                Configuration = olapEtl,
                                TaskState = GetEtlTaskState(olapEtl),
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, olapEtl, out var olapNode, out var olapEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = olapNode,
                                    NodeUrl = clusterTopology.GetUrlFromTag(olapNode)
                                },
                                Error = olapEtlError
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

                        case OngoingTaskType.ElasticSearchEtl:

                            var elasticSearchEtl = name != null ?
                                record.ElasticSearchEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.ElasticSearchEtls?.Find(x => x.TaskId == key);

                            if (elasticSearchEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            await WriteResult(context, new OngoingTaskElasticSearchEtlDetails
                            {
                                TaskId = elasticSearchEtl.TaskId,
                                TaskName = elasticSearchEtl.Name,
                                Configuration = elasticSearchEtl,
                                TaskState = GetEtlTaskState(elasticSearchEtl),
                                MentorNode = elasticSearchEtl.MentorNode,
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, elasticSearchEtl, out var queueNode, out var queueEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = queueNode,
                                    NodeUrl = clusterTopology.GetUrlFromTag(queueNode)
                                },
                                Error = queueEtlError
                            });
                            break;

                        case OngoingTaskType.QueueEtl:

                            var queueEtl = name != null ?
                                record.QueueEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.QueueEtls?.Find(x => x.TaskId == key);

                            if (queueEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            await WriteResult(context, new OngoingTaskQueueEtlDetails
                            {
                                TaskId = queueEtl.TaskId,
                                TaskName = queueEtl.Name,
                                Configuration = queueEtl,
                                TaskState = GetEtlTaskState(queueEtl),
                                MentorNode = queueEtl.MentorNode,
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, queueEtl, out var nodeES, out var elasticSearchEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = nodeES,
                                    NodeUrl = clusterTopology.GetUrlFromTag(nodeES)
                                },
                                Error = elasticSearchEtlError
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
                            var tag = OngoingTasksUtils.WhoseTaskIsIt(ServerStore, record.Topology, subscriptionState, subscriptionState, Database.NotificationCenter);
                            OngoingTaskConnectionStatus connectionStatus = OngoingTaskConnectionStatus.NotActive;
                            if (tag != ServerStore.NodeTag)
                            {
                                connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
                            }
                            else if (Database.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(subscriptionState.SubscriptionId, out var connectionsState))
                            {
                                connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
                            }

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
                                PinToMentorNode = subscriptionState.PinToMentorNode,
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = tag,
                                    NodeUrl = clusterTopology.GetUrlFromTag(tag)
                                },
                                TaskConnectionStatus = connectionStatus
                            };

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

            var description = (disable) ? "disable" : "enable";
            description += $"-{typeStr}-Task {(String.IsNullOrEmpty(taskName) ? String.Empty : $" with task name: '{taskName}'")}";
            LogTaskToAudit(description, key, configuration: null);

        }

        public const string UpdateExternalReplicationDebugTag = "update_external_replication";

        [RavenAction("/databases/*/admin/tasks/external-replication", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task UpdateExternalReplication()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                ExternalReplication watcher = null;
                await DatabaseConfigurations(
                    (_, databaseName, blittableJson, guid) => ServerStore.UpdateExternalReplication(databaseName, blittableJson, guid, out watcher), UpdateExternalReplicationDebugTag,
                    GetRaftRequestIdFromQuery(),
                    fillJson: (json, _, index) =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            var topology = ServerStore.Cluster.ReadDatabaseTopology(context, Database.Name);
                            var taskStatus = ReplicationLoader.GetExternalReplicationState(ServerStore, Database.Name, watcher.TaskId);
                            json[nameof(OngoingTask.ResponsibleNode)] = OngoingTasksUtils.WhoseTaskIsIt(ServerStore, topology, watcher, taskStatus, Database.NotificationCenter);
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

            var description = $"delete-{typeStr}";
            description += $"{(String.IsNullOrEmpty(taskName) ? String.Empty : $" with task name: '{taskName}'")}";
            LogTaskToAudit(description, id, configuration: null);
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

                using (context.Transaction == null ? context.OpenReadTransaction() : null)
                using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, database.Name))
                {
                    if (rawRecord == null)
                        return;

                    switch (type)
                    {
                        case OngoingTaskType.RavenEtl:
                            var ravenEtls = rawRecord.RavenEtls;
                            var ravenEtl = ravenEtls?.Find(x => x.TaskId == id);
                            if (ravenEtl != null)
                                _deletingEtl = (ravenEtl.Name, ravenEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.SqlEtl:
                            var sqlEtls = rawRecord.SqlEtls;
                            var sqlEtl = sqlEtls?.Find(x => x.TaskId == id);
                            if (sqlEtl != null)
                                _deletingEtl = (sqlEtl.Name, sqlEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.OlapEtl:
                            var olapEtls = rawRecord.OlapEtls;
                            var olapEtl = olapEtls?.Find(x => x.TaskId == id);
                            if (olapEtl != null)
                                _deletingEtl = (olapEtl.Name, olapEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.ElasticSearchEtl:
                            var elasticEtls = rawRecord.ElasticSearchEtls;
                            var elasticEtl = elasticEtls?.Find(x => x.TaskId == id);
                            if (elasticEtl != null)
                                _deletingEtl = (elasticEtl.Name, elasticEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                        case OngoingTaskType.QueueEtl:
                            var queueEtls = rawRecord.QueueEtls;
                            var queueEtl = queueEtls?.Find(x => x.TaskId == id);
                            if (queueEtl != null)
                                _deletingEtl = (queueEtl.Name, queueEtl.Transforms.Where(x => string.IsNullOrEmpty(x.Name) == false).Select(x => x.Name).ToList());
                            break;
                    }
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
