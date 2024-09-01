using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.QueueSink;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;
using Voron;
using Size = Sparrow.Size;

namespace Raven.Server.Dashboard
{
    public sealed class DatabasesInfoRetriever : MetricCacher
    {
        private readonly ServerStore _serverStore;
        private readonly CanAccessDatabase _canAccessDatabase;
        private const string DatabasesInfoKey = "DatabasesInfo";

        public DatabasesInfoRetriever(ServerStore serverStore, CanAccessDatabase canAccessDatabase)
        {
            _serverStore = serverStore;
            _canAccessDatabase = canAccessDatabase;

            Initialize();
        }

        public static TimeSpan RefreshRate { get; } = TimeSpan.FromSeconds(3);

        public void Initialize()
        {
            Register(DatabasesInfoKey, TimeSpan.FromSeconds(3), CreateDatabasesInfo);
        }

        private List<AbstractDashboardNotification> CreateDatabasesInfo()
        {
            List<AbstractDashboardNotification> result = FetchDatabasesInfo(_serverStore, _canAccessDatabase, true, _serverStore.ServerShutdown).ToList();

            return result;
        }

        public DatabasesInfo GetDatabasesInfo()
        {
            return GetValue<List<AbstractDashboardNotification>>(DatabasesInfoKey).OfType<DatabasesInfo>().First();
        }

        public DatabasesOngoingTasksInfo GetDatabasesOngoingTasksInfo()
        {
            return GetValue<List<AbstractDashboardNotification>>(DatabasesInfoKey).OfType<DatabasesOngoingTasksInfo>().First();
        }

        public IndexingSpeed GetIndexingSpeed()
        {
            return GetValue<List<AbstractDashboardNotification>>(DatabasesInfoKey).OfType<IndexingSpeed>().First();
        }

        public TrafficWatch GetTrafficWatch()
        {
            return GetValue<List<AbstractDashboardNotification>>(DatabasesInfoKey).OfType<TrafficWatch>().First();
        }

        public DrivesUsage GetDrivesUsage()
        {
            return GetValue<List<AbstractDashboardNotification>>(DatabasesInfoKey).OfType<DrivesUsage>().First();
        }

        private sealed class AggregatedWatchInfo
        {
            public readonly DatabasesInfo DatabasesInfo = new DatabasesInfo();
            public readonly DatabasesOngoingTasksInfo DatabasesOngoingTasksInfo = new DatabasesOngoingTasksInfo();
            public readonly IndexingSpeed IndexingSpeed = new IndexingSpeed();
            public readonly TrafficWatch TrafficWatch = new TrafficWatch();
            public readonly DrivesUsage DrivesUsage = new DrivesUsage();
        }

        public static IEnumerable<AbstractDashboardNotification> FetchDatabasesInfo(ServerStore serverStore, CanAccessDatabase isValidFor, bool collectOngoingTasks, CancellationToken token)
        {
            var trafficWatchInfo = new AggregatedWatchInfo();
            var drivesUsage = trafficWatchInfo.DrivesUsage;

            var rate = (int)RefreshRate.TotalSeconds;
            trafficWatchInfo.TrafficWatch.RequestsPerSecond = (int)Math.Ceiling(serverStore.Server.Metrics.Requests.RequestsPerSec.GetRate(rate));
            trafficWatchInfo.TrafficWatch.AverageRequestDuration = serverStore.Server.Metrics.Requests.AverageDuration.GetRate();

            using (serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                // 1. Fetch databases info
                foreach (var rawDatabaseRecord in serverStore.Cluster.GetAllRawDatabases(context))
                {
                    var databaseName = rawDatabaseRecord.DatabaseName;

                    if (isValidFor != null && isValidFor(databaseName, false) == false)
                        continue;

                    if (rawDatabaseRecord.IsSharded)
                    {
                        if (serverStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(databaseName, out var databaseTask))
                        {
                            var database = databaseTask.Result;
                            var trafficWatchItem = new TrafficWatchItem
                            {
                                Database = databaseName,
                                RequestsPerSecond = database.Metrics.Requests.RequestsPerSec.GetIntRate(rate),
                                AverageRequestDuration = database.Metrics.Requests.AverageDuration.GetRate(),
                                DocumentWritesPerSecond = database.Metrics.Docs.PutsPerSec.GetIntRate(rate),
                                AttachmentWritesPerSecond = database.Metrics.Attachments.PutsPerSec.GetIntRate(rate),
                                CounterWritesPerSecond = database.Metrics.Counters.PutsPerSec.GetIntRate(rate),
                                TimeSeriesWritesPerSecond = database.Metrics.TimeSeries.PutsPerSec.GetIntRate(rate),
                                DocumentsWriteBytesPerSecond = database.Metrics.Docs.BytesPutsPerSec.GetRate(rate),
                                AttachmentsWriteBytesPerSecond = database.Metrics.Attachments.BytesPutsPerSec.GetRate(rate),
                                CountersWriteBytesPerSecond = database.Metrics.Counters.BytesPutsPerSec.GetRate(rate),
                                TimeSeriesWriteBytesPerSecond = database.Metrics.TimeSeries.BytesPutsPerSec.GetRate(rate)
                            };
                            trafficWatchInfo.TrafficWatch.Items.Add(trafficWatchItem);
                        };
                    }

                    foreach (var rawRecord in rawDatabaseRecord.AsShardsOrNormal())
                    {
                        AddInfoForDatabase(serverStore, collectOngoingTasks, trafficWatchInfo, context, rawRecord, token);
                    }
                }

                // 2. Fetch <system> info
                if (isValidFor == null)
                {
                    var currentSystemHash = serverStore._env.CurrentReadTransactionId;
                    var cachedSystemInfoCopy = CachedSystemInfo;

                    if (currentSystemHash != cachedSystemInfoCopy.Hash || cachedSystemInfoCopy.NextDiskSpaceCheck < SystemTime.UtcNow)
                    {
                        var systemInfo = new SystemInfoCache()
                        {
                            Hash = currentSystemHash,
                            NextDiskSpaceCheck = SystemTime.UtcNow.AddSeconds(30),
                            MountPoints = new List<Client.ServerWide.Operations.MountPointUsage>()
                        };

                        // Get new data
                        var systemEnv = new StorageEnvironmentWithType("<System>", StorageEnvironmentWithType.StorageEnvironmentType.System, serverStore._env);
                        var systemMountPoints = serverStore.GetMountPointUsageDetailsFor(systemEnv, includeTempBuffers: true);

                        foreach (var systemPoint in systemMountPoints)
                        {
                            UpdateMountPoint(serverStore.Configuration.Storage, systemPoint, "<System>", drivesUsage);
                            systemInfo.MountPoints.Add(systemPoint);
                        }

                        // Update the cache
                        Interlocked.Exchange(ref CachedSystemInfo, systemInfo);
                    }
                    else
                    {
                        // Use existing data but update IO stats (which has separate cache)
                        foreach (var systemPoint in cachedSystemInfoCopy.MountPoints)
                        {
                            var driveInfo = systemPoint.DiskSpaceResult.DriveName;
                            var ioStatsResult = serverStore.Server.DiskStatsGetter.Get(driveInfo);
                            if (ioStatsResult != null)
                                systemPoint.IoStatsResult = ServerStore.FillIoStatsResult(ioStatsResult); 
                            UpdateMountPoint(serverStore.Configuration.Storage, systemPoint, "<System>", drivesUsage);
                        }
                    }
                }
            }

            yield return trafficWatchInfo.DatabasesInfo;
            yield return trafficWatchInfo.IndexingSpeed;
            yield return trafficWatchInfo.TrafficWatch;
            yield return trafficWatchInfo.DrivesUsage;
            
            if (collectOngoingTasks)
            {
                yield return trafficWatchInfo.DatabasesOngoingTasksInfo;
            }
        }

        private static void AddInfoForDatabase(ServerStore serverStore, bool collectOngoingTasks, AggregatedWatchInfo trafficWatchInfo,
             ClusterOperationContext context, RawDatabaseRecord rawRecord, CancellationToken token)
        {
            var databasesInfo = trafficWatchInfo.DatabasesInfo;
            var drivesUsage = trafficWatchInfo.DrivesUsage;
            var indexingSpeed = trafficWatchInfo.IndexingSpeed;
            var trafficWatch = trafficWatchInfo.TrafficWatch;
            var databasesOngoingTasksInfo = trafficWatchInfo.DatabasesOngoingTasksInfo;
            var rate = (int)RefreshRate.TotalSeconds;

            try
            {
                var databaseName = rawRecord.DatabaseName;
                var databaseOnline = serverStore.DatabasesLandlord.TryGetDatabaseIfLoaded(databaseName, out var database);
                if (databaseOnline == false)
                {
                    SetOfflineDatabaseInfo(serverStore, context, databaseName, databasesInfo, drivesUsage, rawRecord.IsDisabled);
                    return;
                }

                var indexingSpeedItem = new IndexingSpeedItem
                {
                    Database = database.Name,
                    IndexedPerSecond = database.Metrics.MapIndexes.IndexedPerSec.GetRate(rate),
                    MappedPerSecond = database.Metrics.MapReduceIndexes.MappedPerSec.GetRate(rate),
                    ReducedPerSecond = database.Metrics.MapReduceIndexes.ReducedPerSec.GetRate(rate)
                };
                indexingSpeed.Items.Add(indexingSpeedItem);

                var replicationFactor = rawRecord.Topology.ReplicationFactor;
                var documentsStorage = database.DocumentsStorage;
                var indexStorage = database.IndexStore;

                var trafficWatchItem = new TrafficWatchItem
                {
                    Database = database.Name,
                    RequestsPerSecond = database.Metrics.Requests.RequestsPerSec.GetIntRate(rate),
                    AverageRequestDuration = database.Metrics.Requests.AverageDuration.GetRate(),
                    DocumentWritesPerSecond = database.Metrics.Docs.PutsPerSec.GetIntRate(rate),
                    AttachmentWritesPerSecond = database.Metrics.Attachments.PutsPerSec.GetIntRate(rate),
                    CounterWritesPerSecond = database.Metrics.Counters.PutsPerSec.GetIntRate(rate),
                    TimeSeriesWritesPerSecond = database.Metrics.TimeSeries.PutsPerSec.GetIntRate(rate),
                    DocumentsWriteBytesPerSecond = database.Metrics.Docs.BytesPutsPerSec.GetRate(rate),
                    AttachmentsWriteBytesPerSecond = database.Metrics.Attachments.BytesPutsPerSec.GetRate(rate),
                    CountersWriteBytesPerSecond = database.Metrics.Counters.BytesPutsPerSec.GetRate(rate),
                    TimeSeriesWriteBytesPerSecond = database.Metrics.TimeSeries.BytesPutsPerSec.GetRate(rate)
                };
                trafficWatch.Items.Add(trafficWatchItem);

                var ongoingTasksInfoItem = GetOngoingTasksInfoItem(database, serverStore, context, out var ongoingTasksCount);
                if (collectOngoingTasks)
                {
                    databasesOngoingTasksInfo.Items.Add(ongoingTasksInfoItem);
                }

                // TODO: RavenDB-17004 - hash should report on all relevant info 
                var currentEnvironmentsHash = database.GetEnvironmentsHash();

                if (CachedDatabaseInfo.TryGetValue(database.Name, out var item) &&
                    item.Hash == currentEnvironmentsHash &&
                    item.Item.OngoingTasksCount == ongoingTasksCount)
                {
                    databasesInfo.Items.Add(item.Item);

                    if (item.NextDiskSpaceCheck < SystemTime.UtcNow)
                    {
                        item.MountPoints = new List<Client.ServerWide.Operations.MountPointUsage>();
                        DiskUsageCheck(item, database, drivesUsage, token);
                    }
                    else
                    {
                        foreach (var cachedMountPoint in item.MountPoints)
                        {
                            UpdateMountPoint(database.Configuration.Storage, cachedMountPoint, database.Name, drivesUsage);
                        }
                    }
                }
                else
                {
                    using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                    using (documentsContext.OpenReadTransaction())
                    {
                        var databaseInfoItem = new DatabaseInfoItem
                        {
                            Database = database.Name,
                            DocumentsCount = documentsStorage.GetNumberOfDocuments(documentsContext),
                            IndexesCount = database.IndexStore.Count,
                            AlertsCount = database.NotificationCenter.GetAlertCount(),
                            PerformanceHintsCount = database.NotificationCenter.GetPerformanceHintCount(),
                            ReplicationFactor = replicationFactor,
                            ErroredIndexesCount = indexStorage.GetIndexes().Count(index => index.State == IndexState.Error),
                            IndexingErrorsCount = indexStorage.GetIndexes().Sum(index => index.GetErrorCount()),
                            BackupInfo = database.PeriodicBackupRunner?.GetBackupInfo(context),
                            OngoingTasksCount = ongoingTasksCount,
                            Online = true
                        };

                        databasesInfo.Items.Add(databaseInfoItem);

                        CachedDatabaseInfo[database.Name] = item = new DatabaseInfoCache { Hash = currentEnvironmentsHash, Item = databaseInfoItem };
                    }

                    DiskUsageCheck(item, database, drivesUsage, token);
                }
            }
            catch (Exception)
            {
                SetOfflineDatabaseInfo(serverStore, context, rawRecord.DatabaseName, databasesInfo, drivesUsage, disabled: false);
            }
        }

        private static DatabaseOngoingTasksInfoItem GetOngoingTasksInfoItem(DocumentDatabase database, ServerStore serverStore, ClusterOperationContext context, out long ongoingTasksCount)
        {
            var dbRecord = database.ReadDatabaseRecord();

            var extRepCount = dbRecord.ExternalReplications.Count;
            long extRepCountOnNode = GetTaskCountOnNode<ExternalReplication>(database, dbRecord, serverStore, dbRecord.ExternalReplications,
                task => ReplicationLoader.GetExternalReplicationState(serverStore, database.Name, task.TaskId));

            long replicationHubCountOnNode = 0;
            var replicationHubCount = database.ReplicationLoader.OutgoingHandlers.Count(x => x is OutgoingPullReplicationHandler);
            replicationHubCountOnNode += replicationHubCount;

            var replicationSinkCount = dbRecord.SinkPullReplications.Count;
            long replicationSinkCountOnNode = GetTaskCountOnNode<PullReplicationAsSink>(database, dbRecord, serverStore, dbRecord.SinkPullReplications, task => null);

            var ravenEtlCount = database.EtlLoader.RavenDestinations.Count;
            long ravenEtlCountOnNode = GetTaskCountOnNode<RavenEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.RavenDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name));

            var sqlEtlCount = database.EtlLoader.SqlDestinations.Count;
            long sqlEtlCountOnNode = GetTaskCountOnNode<SqlEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.SqlDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name));

            var elasticSearchEtlCount = database.EtlLoader.ElasticSearchDestinations.Count;
            long elasticSearchEtlCountOnNode = GetTaskCountOnNode<ElasticSearchEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.ElasticSearchDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name));

            var olapEtlCount = database.EtlLoader.OlapDestinations.Count;
            long olapEtlCountOnNode = GetTaskCountOnNode<OlapEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.OlapDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name));

            var kafkaEtlCount = database.EtlLoader.GetQueueDestinationCountByBroker(QueueBrokerType.Kafka);
            long kafkaEtlCountOnNode = GetTaskCountOnNode<QueueEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.QueueDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name), task => task.BrokerType == QueueBrokerType.Kafka);
            
            var rabbitMqEtlCount = database.EtlLoader.GetQueueDestinationCountByBroker(QueueBrokerType.RabbitMq);
            long rabbitMqEtlCountOnNode = GetTaskCountOnNode<QueueEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.QueueDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name), task => task.BrokerType == QueueBrokerType.RabbitMq);
            
            var azureQueueStorageEtlCount = database.EtlLoader.GetQueueDestinationCountByBroker(QueueBrokerType.AzureQueueStorage);
            long azureQueueStorageEtlCountOnNode = GetTaskCountOnNode<QueueEtlConfiguration>(database, dbRecord, serverStore, database.EtlLoader.QueueDestinations,
                task => EtlLoader.GetProcessState(task.Transforms, database, task.Name), task => task.BrokerType == QueueBrokerType.AzureQueueStorage);
            
            var periodicBackupCount = database.PeriodicBackupRunner.PeriodicBackups.Count;
            long periodicBackupCountOnNode = BackupUtils.GetTasksCountOnNode(serverStore, database.Name, context);

            var subscriptionCount = database.SubscriptionStorage.GetAllSubscriptionsCount();
            long subscriptionCountOnNode = GetSubscriptionCountOnNode(database, dbRecord, serverStore, context);

            var kafkaSinkCount = database.QueueSinkLoader.GetSinkCountByBroker(QueueBrokerType.Kafka);
            long kafkaSinkCountOnNode = GetTaskCountOnNode<Client.Documents.Operations.QueueSink.QueueSinkConfiguration>(database, dbRecord, serverStore, database.QueueSinkLoader.Sinks,
                task => QueueSinkLoader.GetProcessState(task.Scripts, database, task.Name), task => task.BrokerType == QueueBrokerType.Kafka);

            var rabbitMqSinkCount = database.QueueSinkLoader.GetSinkCountByBroker(QueueBrokerType.RabbitMq);
            long rabbitMqSinkCountOnNode = GetTaskCountOnNode<Client.Documents.Operations.QueueSink.QueueSinkConfiguration>(database, dbRecord, serverStore, database.QueueSinkLoader.Sinks,
                task => QueueSinkLoader.GetProcessState(task.Scripts, database, task.Name), task => task.BrokerType == QueueBrokerType.RabbitMq);

            ongoingTasksCount = extRepCount + replicationHubCount + replicationSinkCount +
                                ravenEtlCount + sqlEtlCount + elasticSearchEtlCount + olapEtlCount + kafkaEtlCount +
                                rabbitMqEtlCount + azureQueueStorageEtlCount + periodicBackupCount + subscriptionCount +
                                kafkaSinkCount + rabbitMqSinkCount;

            return new DatabaseOngoingTasksInfoItem
            {
                Database = database.Name,
                ExternalReplicationCount = extRepCountOnNode,
                ReplicationHubCount = replicationHubCountOnNode,
                ReplicationSinkCount = replicationSinkCountOnNode,
                RavenEtlCount = ravenEtlCountOnNode,
                SqlEtlCount = sqlEtlCountOnNode,
                ElasticSearchEtlCount = elasticSearchEtlCountOnNode,
                OlapEtlCount = olapEtlCountOnNode,
                KafkaEtlCount = kafkaEtlCountOnNode,
                RabbitMqEtlCount = rabbitMqEtlCountOnNode,
                AzureQueueStorageEtlCount = azureQueueStorageEtlCountOnNode,
                PeriodicBackupCount = periodicBackupCountOnNode,
                SubscriptionCount = subscriptionCountOnNode,
                KafkaSinkCount = kafkaSinkCountOnNode,
                RabbitMqSinkCount = rabbitMqSinkCountOnNode,
            };
        }

        private static long GetTaskCountOnNode<T>(DocumentDatabase database,
            DatabaseRecord dbRecord, ServerStore serverStore, IEnumerable<IDatabaseTask> tasks,
            Func<T, IDatabaseTaskStatus> getTaskStatus, Func<T, bool> filter = null) where T : IDatabaseTask
        {
            long taskCountOnNode = 0;
            foreach (var task in tasks)
            {
                if (filter != null && filter((T)task) == false)
                    continue;

                var state = getTaskStatus((T)task);
                var taskTag = OngoingTasksUtils.WhoseTaskIsIt(serverStore, dbRecord.Topology, task, state, database.NotificationCenter);
                if (serverStore.NodeTag == taskTag)
                {
                    taskCountOnNode++;
                }
            }
            return taskCountOnNode;
        }

        private static long GetSubscriptionCountOnNode(DocumentDatabase database, DatabaseRecord dbRecord, ServerStore serverStore, ClusterOperationContext context)
        {
            long taskCountOnNode = 0;
            foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(database.Name)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
                var taskTag = OngoingTasksUtils.WhoseTaskIsIt(serverStore, dbRecord.Topology, subscriptionState, subscriptionState, database.NotificationCenter);
                if (serverStore.NodeTag == taskTag)
                {
                    taskCountOnNode++;
                }
            }

            return taskCountOnNode;
        }

        private static readonly ConcurrentDictionary<string, DatabaseInfoCache> CachedDatabaseInfo =
            new ConcurrentDictionary<string, DatabaseInfoCache>(StringComparer.OrdinalIgnoreCase);

        private sealed class DatabaseInfoCache
        {
            public long Hash;
            public DatabaseInfoItem Item;
            public List<Client.ServerWide.Operations.MountPointUsage> MountPoints = new List<Client.ServerWide.Operations.MountPointUsage>();
            public DateTime NextDiskSpaceCheck;
        }

        private static SystemInfoCache CachedSystemInfo = new SystemInfoCache();

        private sealed class SystemInfoCache
        {
            public long Hash;
            public List<Client.ServerWide.Operations.MountPointUsage> MountPoints = new List<Client.ServerWide.Operations.MountPointUsage>();
            public DateTime NextDiskSpaceCheck;
        }

        private static void DiskUsageCheck(DatabaseInfoCache item, DocumentDatabase database, DrivesUsage drivesUsage, CancellationToken token)
        {
            foreach (var mountPointUsage in database.GetMountPointsUsage(includeTempBuffers: true))
            {
                if (token.IsCancellationRequested)
                    return;

                UpdateMountPoint(database.Configuration.Storage, mountPointUsage, database.Name, drivesUsage);
                item.MountPoints.Add(mountPointUsage);
            }

            item.NextDiskSpaceCheck = SystemTime.UtcNow.AddSeconds(30);
        }

        private static void UpdateMountPoint(StorageConfiguration storageConfiguration, Client.ServerWide.Operations.MountPointUsage mountPointUsage,
            string databaseName, DrivesUsage drivesUsage)
        {
            var mountPoint = mountPointUsage.DiskSpaceResult.DriveName;
            var usage = drivesUsage.Items.FirstOrDefault(x => x.MountPoint == mountPoint);
            if (usage == null)
            {
                usage = new MountPointUsage
                {
                    MountPoint = mountPoint,
                };
                drivesUsage.Items.Add(usage);
            }

            usage.VolumeLabel = mountPointUsage.DiskSpaceResult.VolumeLabel;
            usage.FreeSpace = mountPointUsage.DiskSpaceResult.TotalFreeSpaceInBytes;
            usage.TotalCapacity = mountPointUsage.DiskSpaceResult.TotalSizeInBytes;
            usage.IoStatsResult = mountPointUsage.IoStatsResult;
            usage.IsLowSpace = StorageSpaceMonitor.IsLowSpace(new Size(usage.FreeSpace, SizeUnit.Bytes), new Size(usage.TotalCapacity, SizeUnit.Bytes), storageConfiguration, out string _);

            var existingDatabaseUsage = usage.Items.FirstOrDefault(x => x.Database == databaseName);
            if (existingDatabaseUsage == null)
            {
                existingDatabaseUsage = new DatabaseDiskUsage
                {
                    Database = databaseName
                };
                usage.Items.Add(existingDatabaseUsage);
            }

            existingDatabaseUsage.Size += mountPointUsage.UsedSpace;
            existingDatabaseUsage.TempBuffersSize += mountPointUsage.UsedSpaceByTempBuffers;
        }

        private static void SetOfflineDatabaseInfo(
            ServerStore serverStore,
            ClusterOperationContext context,
            string databaseName,
            DatabasesInfo existingDatabasesInfo,
            DrivesUsage existingDrivesUsage,
            bool disabled)
        {
            using (var databaseRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
            {
                if (databaseRecord == null)
                {
                    // database doesn't exist
                    return;
                }

                var databaseTopology = databaseRecord.Topology;

                var irrelevant = databaseTopology == null ||
                                 databaseTopology.AllNodes.Contains(serverStore.NodeTag) == false;
                var databaseInfoItem = new DatabaseInfoItem
                {
                    Database = databaseName,
                    Online = false,
                    Disabled = disabled,
                    Irrelevant = irrelevant
                };

                if (irrelevant == false)
                {
                    // nothing to fetch if irrelevant on this node
                    UpdateDatabaseInfo(databaseRecord, serverStore, databaseName, existingDrivesUsage, databaseInfoItem);
                }

                existingDatabasesInfo.Items.Add(databaseInfoItem);
            }
        }

        private static void UpdateDatabaseInfo(RawDatabaseRecord databaseRecord, ServerStore serverStore, string databaseName, DrivesUsage existingDrivesUsage,
            DatabaseInfoItem databaseInfoItem)
        {
            ExtendedDatabaseInfo databaseInfo = null;
            if (serverStore.DatabaseInfoCache.TryGet(databaseName, databaseInfoJson =>
            {
                databaseInfo = JsonDeserializationServer.DatabaseInfo(databaseInfoJson);
            }) == false)
                return;

            Debug.Assert(databaseInfo != null);
            var databaseTopology = databaseRecord.Topology;
            var indexesCount = databaseRecord.CountOfIndexes;

            databaseInfoItem.DocumentsCount = databaseInfo.DocumentsCount ?? 0;
            databaseInfoItem.IndexesCount = databaseInfo.IndexesCount ?? indexesCount;
            databaseInfoItem.ReplicationFactor = databaseTopology?.ReplicationFactor ?? databaseInfo.ReplicationFactor;
            databaseInfoItem.ErroredIndexesCount = databaseInfo.IndexingErrors ?? 0;

            if (databaseInfo.MountPointsUsage == null)
                return;

            foreach (var mountPointUsage in databaseInfo.MountPointsUsage)
            {
                var driveName = mountPointUsage.DiskSpaceResult.DriveName;
                var diskSpaceResult = DiskUtils.GetDiskSpaceInfo(
                    mountPointUsage.DiskSpaceResult.DriveName,
                    new DriveInfoBase
                    {
                        DriveName = driveName
                    });

                if (diskSpaceResult != null)
                {
                    // update the latest drive info
                    mountPointUsage.DiskSpaceResult = new Client.ServerWide.Operations.DiskSpaceResult
                    {
                        DriveName = diskSpaceResult.DriveName,
                        VolumeLabel = diskSpaceResult.VolumeLabel,
                        TotalFreeSpaceInBytes = diskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Bytes),
                        TotalSizeInBytes = diskSpaceResult.TotalSize.GetValue(SizeUnit.Bytes)
                    };
                }
                
                var diskStatsResult = serverStore.Server.DiskStatsGetter.Get(driveName);
                if (diskStatsResult != null)
                {
                    mountPointUsage.IoStatsResult = new IoStatsResult
                    {
                        IoReadOperations = diskStatsResult.IoReadOperations,
                        IoWriteOperations = diskStatsResult.IoWriteOperations,
                        ReadThroughputInKb = diskStatsResult.ReadThroughput.GetValue(SizeUnit.Kilobytes),
                        WriteThroughputInKb = diskStatsResult.WriteThroughput.GetValue(SizeUnit.Kilobytes),
                        QueueLength = diskStatsResult.QueueLength,
                    };
                }
                
                UpdateMountPoint(serverStore.Configuration.Storage, mountPointUsage, databaseName, existingDrivesUsage);
            }
        }
    }
    
    public sealed class ExtendedDatabaseInfo : DatabaseInfo
    {
        public List<Client.ServerWide.Operations.MountPointUsage> MountPointsUsage { get; set; }
    }
}
