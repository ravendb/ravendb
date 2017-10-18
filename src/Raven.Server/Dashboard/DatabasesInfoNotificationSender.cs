using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Background;
using Raven.Server.Documents;
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

                FetchDatabasesInfo();
            }
            finally
            {
                _lastSentNotification = DateTime.UtcNow;
            }
        }

        private void FetchDatabasesInfo()
        {
            var databasesInfo = new DatabasesInfo();
            var indexingSpeed = new IndexingSpeed();
            var trafficWatch = new TrafficWatch();
            var drivesUsage = new DrivesUsage();

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionContext))
            using (transactionContext.OpenReadTransaction())
            {
                foreach (var databaseTuple in _serverStore.Cluster.ItemsStartingWith(transactionContext, Constants.Documents.Prefix, 0, int.MaxValue))
                {
                    var databaseName = databaseTuple.ItemName.Substring(3);
                    if (Cts.IsCancellationRequested)
                        return;

                    if (_serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out var databaseTask) == false)
                    {
                        // database does not exist in this server or disabled
                        continue;
                    }

                    var databaseOnline = IsDatabaseOnline(databaseTask, out var database);
                    if (databaseOnline == false)
                    {
                        var databaseInfoItem = new DatabaseInfoItem
                        {
                            Database = databaseName,
                            Online = false
                        };
                        databasesInfo.Items.Add(databaseInfoItem);
                        continue;
                    }

                    var indexingSpeedItem = new IndexingSpeedItem
                    {
                        Database = database.Name,
                        IndexedPerSecond = database.Metrics.MapIndexes.IndexedPerSec.OneMinuteRate,
                        MappedPerSecond = database.Metrics.MapReduceIndexes.MappedPerSec.OneMinuteRate,
                        ReducedPerSecond = database.Metrics.MapReduceIndexes.ReducedPerSec.OneMinuteRate
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
                        RequestsPerSecond = (int)database.Metrics.Requests.RequestsPerSec.OneMinuteRate,
                        WritesPerSecond = (int)database.Metrics.Docs.PutsPerSec.OneMinuteRate,
                        WriteBytesPerSecond = database.Metrics.Docs.BytesPutsPerSec.OneMinuteRate
                    };
                    trafficWatch.Items.Add(trafficWatchItem);
                }
            }

            foreach (var watcher in _watchers)
            {
                // serialize to avoid race conditions
                // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                if (databasesInfo.Items.Count > 0)
                    watcher.NotificationsQueue.Enqueue(databasesInfo.ToJson());
                if (indexingSpeed.Items.Count > 0)
                    watcher.NotificationsQueue.Enqueue(indexingSpeed.ToJson());
                if (trafficWatch.Items.Count > 0)
                    watcher.NotificationsQueue.Enqueue(trafficWatch.ToJson());
                if (drivesUsage.Items.Count > 0)
                    watcher.NotificationsQueue.Enqueue(drivesUsage.ToJson());
            }
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
