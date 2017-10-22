using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Background;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class DatabasesInfoNotificationSender : BackgroundWorkBase
    {
        private readonly ServerStore _serverStore;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;
        private DateTime _lastSentNotification = DateTime.MinValue;

        public DatabasesInfoNotificationSender(string resourceName, ServerStore serverStore,
            ConcurrentSet<ConnectedWatcher> watchers, TimeSpan notificationsThrottle, CancellationToken shutdown) 
            : base(resourceName, shutdown)
        {
            _serverStore = serverStore;
            _watchers = watchers;
            _notificationsThrottle = notificationsThrottle;
        }

        protected override async Task DoWork()
        {
            var now = DateTime.UtcNow;
            var timeSpan = now - _lastSentNotification;
            if (timeSpan < _notificationsThrottle)
            {
                await WaitOrThrowOperationCanceled(_notificationsThrottle - timeSpan);
            }

            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                if (_watchers.Count == 0)
                    return;

                var databasesInfo = FetchDatabasesInfo(_serverStore, Cts).ToList();
                foreach (var watcher in _watchers)
                {
                    foreach (var info in databasesInfo)
                    {
                        // serialize to avoid race conditions
                        // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                        watcher.NotificationsQueue.Enqueue(info.ToJson());
                    }
                }
            }
            finally
            {
                _lastSentNotification = DateTime.UtcNow;
            }
        }

        public static IEnumerable<AbstractDashboardNotification> FetchDatabasesInfo(ServerStore serverStore, CancellationTokenSource cts)
        {
            var databasesInfo = new DatabasesInfo();
            var indexingSpeed = new IndexingSpeed();
            var trafficWatch = new TrafficWatch();
            var drivesUsage = new DrivesUsage();

            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionContext))
            using (transactionContext.OpenReadTransaction())
            {
                foreach (var databaseTuple in serverStore.Cluster.ItemsStartingWith(transactionContext, Constants.Documents.Prefix, 0, int.MaxValue))
                {
                    var databaseName = databaseTuple.ItemName.Substring(3);
                    if (cts.IsCancellationRequested)
                        yield break;

                    if (serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out var databaseTask) == false)
                    {
                        // database does not exist in this server or disabled
                        continue;
                    }

                    var databaseOnline = IsDatabaseOnline(databaseTask, out var database);
                    if (databaseOnline == false)
                    {
                        var databaseRecord = serverStore.LoadDatabaseRecord(databaseName, out var _);
                        if (databaseRecord == null)
                        {
                            // database doesn't exist
                            continue;
                        }

                        var databaseInfoItem = new DatabaseInfoItem
                        {
                            Database = databaseName,
                            Online = false
                        };

                        DatabaseInfo databaseInfo = null;
                        if (serverStore.DatabaseInfoCache.TryGet(databaseName, 
                            databaseInfoJson => databaseInfo = JsonDeserializationServer.DatabaseInfo(databaseInfoJson)))
                        {
                            Debug.Assert(databaseInfo != null);

                            databaseInfoItem.DocumentsCount = databaseInfo.DocumentsCount ?? 0;
                            databaseInfoItem.IndexesCount = databaseInfo.IndexesCount ?? databaseRecord.Indexes.Count;
                            databaseInfoItem.ReplicationFactor = databaseRecord.Topology?.ReplicationFactor ?? databaseInfo.ReplicationFactor;
                            databaseInfoItem.ErroredIndexesCount = databaseInfo.IndexingErrors ?? 0;
                        }

                        databasesInfo.Items.Add(databaseInfoItem);
                        continue;
                    }

                    var indexingSpeedItem = new IndexingSpeedItem
                    {
                        Database = database.Name,
                        IndexedPerSecond = database.Metrics.MapIndexes.IndexedPerSec.FiveSecondRate,
                        MappedPerSecond = database.Metrics.MapReduceIndexes.MappedPerSec.FiveSecondRate,
                        ReducedPerSecond = database.Metrics.MapReduceIndexes.ReducedPerSec.FiveSecondRate
                    };
                    indexingSpeed.Items.Add(indexingSpeedItem);

                    var replicationFactor = GetReplicationFactor(databaseTuple.Value);
                    var documentsStorage = database.DocumentsStorage;
                    var indexStorage = database.IndexStore;
                    using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                    using (documentsContext.OpenReadTransaction())
                    {
                        var databaseInfoItem = new DatabaseInfoItem
                        {
                            Database = databaseName,
                            DocumentsCount = documentsStorage.GetNumberOfDocuments(documentsContext),
                            IndexesCount = database.IndexStore.Count,
                            AlertsCount = database.NotificationCenter.GetAlertCount(),
                            ReplicationFactor = replicationFactor,
                            ErroredIndexesCount = indexStorage.GetIndexes().Count(index => index.GetErrorCount() > 0),
                            Online = true
                        };
                        databasesInfo.Items.Add(databaseInfoItem);
                    }

                    var trafficWatchItem = new TrafficWatchItem
                    {
                        Database = databaseName,
                        RequestsPerSecond = (int)database.Metrics.Requests.RequestsPerSec.FiveSecondRate,
                        WritesPerSecond = (int)database.Metrics.Docs.PutsPerSec.FiveSecondRate,
                        WriteBytesPerSecond = database.Metrics.Docs.BytesPutsPerSec.FiveSecondRate
                    };
                    trafficWatch.Items.Add(trafficWatchItem);

                    foreach (var mountPointUsage in database.GetMountPointsUsage())
                    {
                        if (cts.IsCancellationRequested)
                            yield break;

                        var usage = drivesUsage.Items.FirstOrDefault(x => x.MountPoint == mountPointUsage.Drive.Name);
                        if (usage == null)
                        {
                            usage = new MountPointUsage
                            {
                                MountPoint = mountPointUsage.Drive.Name,
                                VolumeLabel = mountPointUsage.Drive.VolumeLabel,
                                FreeSpace = mountPointUsage.Drive.AvailableFreeSpace,
                                TotalCapacity = mountPointUsage.Drive.TotalSize
                            };
                            drivesUsage.Items.Add(usage);
                        }

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
                    }
                }
            }

            yield return databasesInfo;
            yield return indexingSpeed;
            yield return trafficWatch;
            yield return drivesUsage;
            
        }

        private static int GetReplicationFactor(BlittableJsonReaderObject databaseRecordBlittable)
        {
            if (databaseRecordBlittable.TryGet("Topology", out BlittableJsonReaderObject topology) == false)
                return 1;

            if (topology.TryGet("ReplicationFactor", out int replicationFactor) == false)
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
