using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class Indexing : IDisposable
    {
        private static readonly string Source = "Indexing";
        private static readonly string IndexingTableId = PerformanceHint.GetKey(PerformanceHintType.Indexing, Source);

        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        private DateTime _warningUpdateTime = new DateTime();
        private ConcurrentQueue<(string Action, WarnIndexOutputsPerDocument.WarningDetails warningDetails)> _warningQueue = 
            new ConcurrentQueue<(string Action, WarnIndexOutputsPerDocument.WarningDetails warningDetails)>();
        
        private Timer _indexingTimer;
        private readonly Logger _logger;
        private readonly object _locker = new object();

        public Indexing(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
        }

        public void AddWarning(string indexName, WarnIndexOutputsPerDocument.WarningDetails _indexOutputsWarning)
        {
            var now = SystemTime.UtcNow;
            var update = _warningUpdateTime;

            if (now - update < TimeSpan.FromSeconds(15))
                return;

            _warningUpdateTime = now;
            
            _indexOutputsWarning.LastWarningTime = now;
            _warningQueue.Enqueue((indexName, _indexOutputsWarning));

            while (_warningQueue.Count > 50)
                _warningQueue.TryDequeue(out _);

            if (_indexingTimer != null)
                return;

            lock (_locker)
            {
                if (_indexingTimer != null)
                    return;

                _indexingTimer = new Timer(UpdateIndexing, null, TimeSpan.FromSeconds(0), TimeSpan.FromMinutes(5)); 
            }
        }

        private void UpdateIndexing(object state)
        {
            try
            {
                if (_warningQueue.IsEmpty)
                    return;

                PerformanceHint indexingWarnings = null;

                while (_warningQueue.TryDequeue(
                    out (string indexName, WarnIndexOutputsPerDocument.WarningDetails warningDetails) tuple))
                {
                    if (indexingWarnings == null)
                        indexingWarnings = GetIndexingPerformanceHint(IndexingTableId);

                    ((WarnIndexOutputsPerDocument)indexingWarnings.Details).Update(tuple.indexName, tuple.warningDetails);
                }

                if (indexingWarnings != null)
                    _notificationCenter.Add(indexingWarnings);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in the notification center indexing timer", e);
            }
        }

        private PerformanceHint GetIndexingPerformanceHint(string IndexingTableId)
        {
            using (_notificationsStorage.Read(IndexingTableId, out NotificationTableValue ntv))
            {
                WarnIndexOutputsPerDocument details;
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    details = new WarnIndexOutputsPerDocument();
                else
                    details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<WarnIndexOutputsPerDocument>(detailsJson, IndexingTableId);

                return PerformanceHint.Create(_database, "High indexing fanout ratio",
                    "Number of map results produced by an index exceeds the performance hint configuration key (MaxIndexOutputsPerDocument).",
                    PerformanceHintType.Indexing, NotificationSeverity.Warning, Source, details);
            }
        }

        public void Dispose()
        {
            _indexingTimer?.Dispose();
        }
    }
}
