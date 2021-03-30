using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server.Utils;
using Voron;
using Size = Sparrow.Size;

namespace Raven.Server.Dashboard
{
    public class DatabasesInfoRetriever : MetricCacher
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

        public TimeSpan RefreshRate { get; } = TimeSpan.FromSeconds(3);

        public void Initialize()
        {
            Register(DatabasesInfoKey, TimeSpan.FromSeconds(3), CreateDatabasesInfo);
        }

        private List<AbstractDashboardNotification> CreateDatabasesInfo()
        {
            List<AbstractDashboardNotification> result = FetchDatabasesInfo(_serverStore, _canAccessDatabase, _serverStore.ServerShutdown).ToList();

            return result;
        }

        public DatabasesInfo GetDatabasesInfo()
        {
            return GetValue<List<AbstractDashboardNotification>>(DatabasesInfoKey).OfType<DatabasesInfo>().First();
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

        public static IEnumerable<AbstractDashboardNotification> FetchDatabasesInfo(ServerStore serverStore, CanAccessDatabase isValidFor, CancellationToken token)
        {
            var databasesInfo = new DatabasesInfo();
            var indexingSpeed = new IndexingSpeed();
            var trafficWatch = new TrafficWatch();
            var drivesUsage = new DrivesUsage();

            trafficWatch.AverageRequestDuration = serverStore.Server.Metrics.Requests.AverageDuration.GetRate();

            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                // 1. Fetch databases info
                foreach (var databaseTuple in serverStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, long.MaxValue))
                {
                    var databaseName = databaseTuple.ItemName.Substring(Constants.Documents.Prefix.Length);
                    if (token.IsCancellationRequested)
                        yield break;

                    if (isValidFor != null && isValidFor(databaseName, false) == false)
                        continue;

                    if (serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out var databaseTask) == false)
                    {
                        // database does not exist on this server, is offline or disabled
                        SetOfflineDatabaseInfo(serverStore, context, databaseName, databasesInfo, drivesUsage, disabled: DatabasesLandlord.IsDatabaseDisabled(databaseTuple.Value));
                        continue;
                    }

                    try
                    {
                        var databaseOnline = IsDatabaseOnline(databaseTask, out var database);
                        if (databaseOnline == false)
                        {
                            SetOfflineDatabaseInfo(serverStore, context, databaseName, databasesInfo, drivesUsage, disabled: false);
                            continue;
                        }

                        var indexingSpeedItem = new IndexingSpeedItem
                        {
                            Database = database.Name,
                            IndexedPerSecond = database.Metrics.MapIndexes.IndexedPerSec.OneSecondRate,
                            MappedPerSecond = database.Metrics.MapReduceIndexes.MappedPerSec.OneSecondRate,
                            ReducedPerSecond = database.Metrics.MapReduceIndexes.ReducedPerSec.OneSecondRate
                        };
                        indexingSpeed.Items.Add(indexingSpeedItem);

                        var replicationFactor = GetReplicationFactor(databaseTuple.Value);
                        var documentsStorage = database.DocumentsStorage;
                        var indexStorage = database.IndexStore;

                        var trafficWatchItem = new TrafficWatchItem
                        {
                            Database = database.Name,
                            RequestsPerSecond = (int)database.Metrics.Requests.RequestsPerSec.OneSecondRate,
                            AverageRequestDuration = database.Metrics.Requests.AverageDuration.GetRate(),
                            DocumentWritesPerSecond = (int)database.Metrics.Docs.PutsPerSec.OneSecondRate,
                            AttachmentWritesPerSecond = (int)database.Metrics.Attachments.PutsPerSec.OneSecondRate,
                            CounterWritesPerSecond = (int)database.Metrics.Counters.PutsPerSec.OneSecondRate,
                            TimeSeriesWritesPerSecond = (int)database.Metrics.TimeSeries.PutsPerSec.OneSecondRate,
                            DocumentsWriteBytesPerSecond = database.Metrics.Docs.BytesPutsPerSec.OneSecondRate,
                            AttachmentsWriteBytesPerSecond = database.Metrics.Attachments.BytesPutsPerSec.OneSecondRate,
                            CountersWriteBytesPerSecond = database.Metrics.Counters.BytesPutsPerSec.OneSecondRate,
                            TimeSeriesWriteBytesPerSecond = database.Metrics.TimeSeries.BytesPutsPerSec.OneSecondRate
                        };
                        trafficWatch.Items.Add(trafficWatchItem);

                        var currentEnvironmentsHash = database.GetEnvironmentsHash();
                        if (CachedDatabaseInfo.TryGetValue(database.Name, out var item) && item.Hash == currentEnvironmentsHash)
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
                                    ReplicationFactor = replicationFactor,
                                    ErroredIndexesCount = indexStorage.GetIndexes().Count(index => index.GetErrorCount() > 0),
                                    Online = true
                                };
                                databasesInfo.Items.Add(databaseInfoItem);
                                CachedDatabaseInfo[database.Name] = item = new DatabaseInfoCache
                                {
                                    Hash = currentEnvironmentsHash,
                                    Item = databaseInfoItem
                                };
                            }

                            DiskUsageCheck(item, database, drivesUsage, token);
                        }
                    }
                    catch (Exception)
                    {
                        SetOfflineDatabaseInfo(serverStore, context, databaseName, databasesInfo, drivesUsage, disabled: false);
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
                        var systemMountPoints = ServerStore.GetMountPointUsageDetailsFor(systemEnv, includeTempBuffers: true);

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
                        // Use existing data
                        foreach (var systemPoint in cachedSystemInfoCopy.MountPoints)
                        {
                            UpdateMountPoint(serverStore.Configuration.Storage, systemPoint, "<System>", drivesUsage);
                        }
                    }
                }
            }

            yield return databasesInfo;
            yield return indexingSpeed;
            yield return trafficWatch;
            yield return drivesUsage;
        }

        private static readonly ConcurrentDictionary<string, DatabaseInfoCache> CachedDatabaseInfo =
            new ConcurrentDictionary<string, DatabaseInfoCache>(StringComparer.OrdinalIgnoreCase);

        private class DatabaseInfoCache
        {
            public long Hash;
            public DatabaseInfoItem Item;
            public List<Client.ServerWide.Operations.MountPointUsage> MountPoints = new List<Client.ServerWide.Operations.MountPointUsage>();
            public DateTime NextDiskSpaceCheck;
        }

        private static SystemInfoCache CachedSystemInfo = new SystemInfoCache();

        private class SystemInfoCache
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
            TransactionOperationContext context,
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
            DatabaseInfo databaseInfo = null;
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
                var diskSpaceResult = DiskSpaceChecker.GetDiskSpaceInfo(
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

                UpdateMountPoint(serverStore.Configuration.Storage, mountPointUsage, databaseName, existingDrivesUsage);
            }
        }

        private static int GetReplicationFactor(BlittableJsonReaderObject databaseRecordBlittable)
        {
            if (databaseRecordBlittable.TryGet(nameof(DatabaseRecord.Topology), out BlittableJsonReaderObject topology) == false)
                return 1;

            if (topology.TryGet(nameof(DatabaseTopology.ReplicationFactor), out int replicationFactor) == false)
                return 1;

            return replicationFactor;
        }

        private static bool IsDatabaseOnline(Task<DocumentDatabase> databaseTask, out DocumentDatabase database)
        {
            if (databaseTask.IsCanceled || databaseTask.IsFaulted || databaseTask.IsCompleted == false)
            {
                database = null;
                return false;
            }

            database = databaseTask.Result;
            return database.DatabaseShutdown.IsCancellationRequested == false;
        }
    }
}
