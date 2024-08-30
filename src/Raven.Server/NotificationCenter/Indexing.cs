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
        private static readonly string HighOutputsRate = PerformanceHint.GetKey(PerformanceHintType.Indexing, Source);
        private static readonly string ReferencesLoad = PerformanceHint.GetKey(PerformanceHintType.Indexing_References, Source);

        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        private DateTime _warningUpdateTime = new DateTime();
        private readonly ConcurrentQueue<(string indexName, WarnIndexOutputsPerDocument.WarningDetails warningDetails)> _warningIndexOutputsPerDocumentQueue = new();

        private readonly ConcurrentQueue<(string indexName, IndexingReferenceLoadWarning.WarningDetails warningDetails)> _warningReferenceDocumentLoadsQueue = new();

        private MismatchedReferencesLoadWarning _mismatchedReferencesLoadWarning;

        private Timer _indexingTimer;
        private readonly Logger _logger;
        private readonly object _locker = new object();

        internal TimeSpan MinUpdateInterval = TimeSpan.FromSeconds(15);

        public Indexing(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
        }

        public void AddWarning(string indexName, WarnIndexOutputsPerDocument.WarningDetails indexOutputsWarning)
        {
            if (CanAdd(out DateTime now) == false)
                return;

            indexOutputsWarning.LastWarningTime = now;
            _warningIndexOutputsPerDocumentQueue.Enqueue((indexName, indexOutputsWarning));

            while (_warningIndexOutputsPerDocumentQueue.Count > 50)
                _warningIndexOutputsPerDocumentQueue.TryDequeue(out _);

            EnsureTimer();
        }

        public void AddWarning(string indexName, IndexingReferenceLoadWarning.WarningDetails referenceLoadsWarning)
        {
            if (CanAdd(out DateTime now) == false)
                return;

            referenceLoadsWarning.LastWarningTime = now;
            _warningReferenceDocumentLoadsQueue.Enqueue((indexName, referenceLoadsWarning));

            while (_warningReferenceDocumentLoadsQueue.Count > 50)
                _warningReferenceDocumentLoadsQueue.TryDequeue(out _);

            EnsureTimer();
        }

        public void AddWarning(MismatchedReferencesLoadWarning mismatchedReferenceLoadWarningDetails)
        {
            if (CanAdd(out DateTime now) == false)
                return;

            _mismatchedReferencesLoadWarning = mismatchedReferenceLoadWarningDetails;

            EnsureTimer();
        }

        private bool CanAdd(out DateTime now)
        {
            now = SystemTime.UtcNow;
            var update = _warningUpdateTime;

            if (now - update < MinUpdateInterval)
                return false;

            _warningUpdateTime = now;

            return true;
        }

        private void EnsureTimer()
        {
            if (_indexingTimer != null)
                return;

            lock (_locker)
            {
                if (_indexingTimer != null)
                    return;

                _indexingTimer = new Timer(UpdateIndexing, null, TimeSpan.FromSeconds(0), TimeSpan.FromMinutes(5));
            }
        }

        internal void UpdateIndexing(object state)
        {
            try
            {
                if (_warningIndexOutputsPerDocumentQueue.IsEmpty && _warningReferenceDocumentLoadsQueue.IsEmpty && _mismatchedReferencesLoadWarning == null)
                    return;

                PerformanceHint indexOutputPerDocumentHint = null;

                while (_warningIndexOutputsPerDocumentQueue.TryDequeue(
                    out (string indexName, WarnIndexOutputsPerDocument.WarningDetails warningDetails) tuple))
                {
                    indexOutputPerDocumentHint ??= GetIndexOutputPerDocumentPerformanceHint();

                    ((WarnIndexOutputsPerDocument)indexOutputPerDocumentHint.Details).Update(tuple.indexName, tuple.warningDetails);
                }

                PerformanceHint referenceLoadsHint = null;

                while (_warningReferenceDocumentLoadsQueue.TryDequeue(
                    out (string indexName, IndexingReferenceLoadWarning.WarningDetails warningDetails) tuple))
                {
                    referenceLoadsHint ??= GetReferenceLoadsPerformanceHint();

                    ((IndexingReferenceLoadWarning)referenceLoadsHint.Details).Update(tuple.indexName, tuple.warningDetails);
                }

                if (indexOutputPerDocumentHint != null)
                    _notificationCenter.Add(indexOutputPerDocumentHint);

                if (referenceLoadsHint != null)
                    _notificationCenter.Add(referenceLoadsHint);

                if (_mismatchedReferencesLoadWarning != null)
                {
                    AlertRaised mismatchedReferencesAlert = GetMismatchedReferencesAlert();
                    _notificationCenter.Add(mismatchedReferencesAlert);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in the notification center indexing timer", e);
            }
        }

        private PerformanceHint GetIndexOutputPerDocumentPerformanceHint()
        {
            using (_notificationsStorage.Read(HighOutputsRate, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    WarnIndexOutputsPerDocument details;
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                        details = new WarnIndexOutputsPerDocument();
                    else
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<WarnIndexOutputsPerDocument>(detailsJson, HighOutputsRate);

                    return PerformanceHint.Create(_database, "High indexing fanout ratio",
                        "Number of map results produced by an index exceeds the performance hint configuration key (MaxIndexOutputsPerDocument).",
                        PerformanceHintType.Indexing, NotificationSeverity.Warning, Source, details);
                }
            }
        }

        private PerformanceHint GetReferenceLoadsPerformanceHint()
        {
            using (_notificationsStorage.Read(ReferencesLoad, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    IndexingReferenceLoadWarning details;
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                        details = new IndexingReferenceLoadWarning();
                    else
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexingReferenceLoadWarning>(detailsJson, ReferencesLoad);

                    return PerformanceHint.Create(_database, "High indexing load reference rate",
                        "We have detected high number of LoadDocument() / LoadCompareExchangeValue() calls per single reference item. The update of a reference will result in reindexing all documents that reference it. " +
                        "Please see Indexing Performance graph to check the performance of your indexes.",
                        PerformanceHintType.Indexing_References, NotificationSeverity.Warning, Source, details);
                }
            }
        }

        private AlertRaised GetMismatchedReferencesAlert()
        {
            return AlertRaised.Create(_database, $"Loading documents with mismatched collection name in '{_mismatchedReferencesLoadWarning.IndexName}' index",
                "We have detected usage of LoadDocument(doc, collectionName) where loaded document collection is different than given parameter.",
                AlertType.MismatchedReferenceLoad, NotificationSeverity.Warning, Source, _mismatchedReferencesLoadWarning);
        }

        public void Dispose()
        {
            _indexingTimer?.Dispose();
        }
    }
}
