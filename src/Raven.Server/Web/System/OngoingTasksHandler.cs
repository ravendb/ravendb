using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Studio.Processors;
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
            connectionStrings.TryGetValue(sinkReplication.ConnectionStringName, out var connection);
            sinkReplication.Database = connection?.Database;
            sinkReplication.ConnectionString = connection;

            var tag = Database.WhoseTaskIsIt(dbTopology, sinkReplication, null);

            (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.NotActive);
            IncomingReplicationHandler handler = null;
            if (tag == ServerStore.NodeTag)
            {
                foreach (var incoming in handlers)
                {
                    if (incoming is IncomingPullReplicationHandler pullHandler &&
                        pullHandler._incomingPullReplicationParams?.Name == sinkReplication.HubName)
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
            var pullReplicationHandlers = Database.ReplicationLoader.OutgoingHandlers.Where(n => n is OutgoingPullReplicationHandler).ToList();
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
            using (var processor = new OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration<DatabaseRequestHandler, DocumentsOperationContext>(this, ContextPool))
                await processor.ExecuteAsync();
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
                beforeSetupConfiguration: (string databaseName, ref BlittableJsonReaderObject readerObject, JsonOperationContext context, ServerStore serverStore) =>
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
            using (var processor = new OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<DatabaseRequestHandler, DocumentsOperationContext>(this, ContextPool, Database.Name))
                await processor.ExecuteAsync();
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

                                    using (Database.PreventFromUnloadingByIdleOperations())
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
            using (var processor = new OngoingTasksHandlerProcessorForDeleteConnectionString<DatabaseRequestHandler, DocumentsOperationContext>(this, ContextPool, Database.Name))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/connection-strings", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetConnectionStrings()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetConnectionString<DatabaseRequestHandler, DocumentsOperationContext>(this, ContextPool, Database.Name))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/connection-strings", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task PutConnectionString()
        {
            using (var processor = new OngoingTasksHandlerProcessorForPutConnectionString<DatabaseRequestHandler, DocumentsOperationContext>(this, ContextPool, Database.Name))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/etl", "RESET", AuthorizationStatus.DatabaseAdmin)]
        public async Task ResetEtl()
        {
            await ResetEtl(Database.Name, this);
        }

        public static async Task ResetEtl(string databaseName, RequestHandler requestHandler)
        {
            var configurationName = requestHandler.GetStringQueryString("configurationName"); // etl task name
            var transformationName = requestHandler.GetStringQueryString("transformationName");

            await DatabaseConfigurations((_, db, etlConfiguration, guid) => requestHandler.ServerStore.RemoveEtlProcessState(_, db, configurationName, transformationName, guid),
                "etl-reset", requestHandler.GetRaftRequestIdFromQuery(), databaseName, requestHandler);
        }

        [RavenAction("/databases/*/admin/etl", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddEtl()
        {
            await AddEtl(Database.Name, this);
        }

        public static async Task AddEtl(string databaseName, RequestHandler requestHandler)
        {
            var id = requestHandler.GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, db, etlConfiguration, guid) =>
                        requestHandler.ServerStore.AddEtl(_, db, etlConfiguration, guid), "etl-add",
                    requestHandler.GetRaftRequestIdFromQuery(),
                    databaseName,
                    requestHandler,
                    beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                    fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            string etlConfigurationName = null;

            await DatabaseConfigurations((_, db, etlConfiguration, guid) =>
                {
                    var task = requestHandler.ServerStore.UpdateEtl(_, db, id.Value, etlConfiguration, guid);
                    etlConfiguration.TryGet(nameof(RavenEtlConfiguration.Name), out etlConfigurationName);
                    return task;
                }, "etl-update",
                requestHandler.GetRaftRequestIdFromQuery(),
                databaseName,
                requestHandler,
                beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

            // Reset scripts if needed
            var scriptsToReset = requestHandler.HttpContext.Request.Query["reset"];
            var raftRequestId = requestHandler.GetRaftRequestIdFromQuery();

            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var script in scriptsToReset)
                {
                    await requestHandler.ServerStore.RemoveEtlProcessState(ctx, databaseName, etlConfigurationName, script, $"{raftRequestId}/{script}");
                }
            }
        }

        private static void AssertCanAddOrUpdateEtl(string databaseName, ref BlittableJsonReaderObject etlConfiguration, JsonOperationContext context, ServerStore serverStore)
        {
            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    serverStore.LicenseManager.AssertCanAddRavenEtl();
                    break;
                case EtlType.Sql:
                    serverStore.LicenseManager.AssertCanAddSqlEtl();
                    break;
                case EtlType.Olap:
                    serverStore.LicenseManager.AssertCanAddOlapEtl();
                    break;
                case EtlType.ElasticSearch:
                    serverStore.LicenseManager.AssertCanAddElasticSearchEtl();
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
            bool sharded = false;
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
            {
                sharded = ShardHelper.IsShardedName(Database.Name);
                if (sharded == false)
                    throw new BadRequestException(errorMessage);
            }
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

                            RavenEtlConfiguration ravenEtl;
                            if (sharded)
                            {
                                var taskName = ShardHelper.ToDatabaseName(name);
                                ravenEtl = record.RavenEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase));
                            }
                            else
                            {
                                ravenEtl = name != null ?
                                    record.RavenEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                    : record.RavenEtls?.Find(x => x.TaskId == key);
                            }

                            if (ravenEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var process = Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);

                            await WriteResult(context, new OngoingTaskRavenEtlDetails
                            {
                                TaskId = ravenEtl.TaskId,
                                TaskName = name ?? ravenEtl.Name,
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
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, elasticSearchEtl, out var nodeES, out var elasticSearchEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = nodeES,
                                    NodeUrl = clusterTopology.GetUrlFromTag(nodeES)
                                },
                                Error = elasticSearchEtlError
                            });
                            break;

                        case OngoingTaskType.Subscription:
                            SubscriptionState subscriptionState = null;
                            if (name == null)
                            {
                                subscriptionState = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateById(context, Database.Name, key);
                            }
                            else
                            {
                                try
                                {
                                    subscriptionState = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, Database.Name, name);
                                }
                                catch (SubscriptionDoesNotExistException)
                                {
                                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                    break;
                                }
                            }

                            var tag = Database.WhoseTaskIsIt(record.Topology, subscriptionState, subscriptionState);
                            OngoingTaskConnectionStatus connectionStatus = OngoingTaskConnectionStatus.NotActive;
                            if (tag != ServerStore.NodeTag)
                            {
                                connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
                            }
                            else if (Database.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(key, out var connectionsState))
                            {
                                connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
                            }

                            var subscriptionStateInfo = new OngoingTaskSubscription
                            {
                                TaskName = subscriptionState.SubscriptionName,
                                TaskId = subscriptionState.SubscriptionId,
                                Query = subscriptionState.Query,
                                ChangeVectorForNextBatchStartingPoint = subscriptionState.ChangeVectorForNextBatchStartingPoint,
                                ChangeVectorForNextBatchStartingPointPerShard = subscriptionState.ChangeVectorForNextBatchStartingPointPerShard,
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
            await ToggleTaskState(Database.Name, this, Database);
        }

        internal static async Task ToggleTaskState(string databaseName, RequestHandler requestHandler, DocumentDatabase database = null)
        {
            if (ResourceNameValidator.IsValidResourceName(databaseName, requestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var key = requestHandler.GetLongQueryString("key");
            var typeStr = requestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var disable = requestHandler.GetBoolValueQueryString("disable") ?? true;
            var taskName = requestHandler.GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var (index, _) = await requestHandler.ServerStore.ToggleTaskState(key, taskName, type, disable, databaseName, requestHandler.GetRaftRequestIdFromQuery());
                if (database == null)
                {
                    // sharded database
                    await requestHandler.WaitForIndexToBeAppliedAsync(context, index);
                }
                else
                {
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index, requestHandler.ServerStore.Engine.OperationTimeout);
                }

                requestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
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
            await DeleteOngoingTask(Database.Name, this, Database);
        }

        internal static async Task DeleteOngoingTask(string databaseName, RequestHandler requestHandler, DocumentDatabase database = null)
        {
            if (ResourceNameValidator.IsValidResourceName(databaseName, requestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            var id = requestHandler.GetLongQueryString("id");
            var typeStr = requestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            var taskName = requestHandler.GetStringQueryString("taskName", required: false);

            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", "type");

            using (requestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                long index;
                bool sharded = false;
                if (database == null)
                {
                    // sharded database
                    database = await requestHandler.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(databaseName).First();
                    sharded = true;
                }

                var action = new DeleteOngoingTaskAction(id, type, requestHandler.ServerStore, database, databaseName, context);
                var raftRequestId = requestHandler.GetRaftRequestIdFromQuery();

                try
                {
                    (index, _) = await requestHandler.ServerStore.DeleteOngoingTask(id, taskName, type, databaseName, $"{raftRequestId}/delete-ongoing-task");
                    if (sharded)
                    {
                        await requestHandler.WaitForIndexToBeAppliedAsync(context, index);
                    }
                    else
                    {
                        await database.RachisLogIndexNotifications.WaitForIndexNotification(index, requestHandler.ServerStore.Engine.OperationTimeout);
                    }

                    if (type == OngoingTaskType.Subscription)
                    {
                        database.SubscriptionStorage.RaiseNotificationForTaskRemoved(taskName);
                    }
                }
                finally
                {
                    await action.Complete($"{raftRequestId}/complete");
                }

                requestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, requestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOngoingTaskResult.TaskId)] = id,
                        [nameof(ModifyOngoingTaskResult.RaftCommandIndex)] = index
                    });
                }
            }
        }

        internal static OngoingTaskState GetEtlTaskState<T>(EtlConfiguration<T> config) where T : ConnectionString
        {
            var taskState = OngoingTaskState.Enabled;

            if (config.Disabled || config.Transforms.All(x => x.Disabled))
                taskState = OngoingTaskState.Disabled;
            else if (config.Transforms.Any(x => x.Disabled))
                taskState = OngoingTaskState.PartiallyEnabled;

            return taskState;
        }

        internal class DeleteOngoingTaskAction
        {
            private readonly ServerStore _serverStore;
            private readonly DocumentDatabase _database;
            private readonly TransactionOperationContext _context;
            private readonly (string Name, List<string> Transformations) _deletingEtl;
            private readonly string _databaseName;

            public DeleteOngoingTaskAction(long id, OngoingTaskType type, ServerStore serverStore, DocumentDatabase database, string databaseName, TransactionOperationContext context)
            {
                _serverStore = serverStore;
                _database = database;
                _context = context;
                _databaseName = databaseName;

                        using (context.Transaction == null ? context.OpenReadTransaction() : null)
                        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, _databaseName))
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
                            }
                        }
                }

            public async Task Complete(string raftRequestId)
            {
                if (_deletingEtl.Name != null)
                {
                    foreach (var transformation in _deletingEtl.Transformations)
                    {
                        var (index, _) = await _serverStore.RemoveEtlProcessState(_context, _databaseName, _deletingEtl.Name, transformation,
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
