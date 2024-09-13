using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public sealed class Indexing : IDisposable
    {
        private static readonly string Source = "Indexing";
        private static readonly string HighOutputsRate = PerformanceHint.GetKey(PerformanceHintType.Indexing, Source);
        private static readonly string ReferencesLoad = PerformanceHint.GetKey(PerformanceHintType.Indexing_References, Source);

        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        private DateTime _warningUpdateTime = new DateTime();
        private readonly ConcurrentQueue<(string indexName, WarnIndexOutputsPerDocument.WarningDetails warningDetails)> _warningIndexOutputsPerDocumentQueue = new();

        private readonly ConcurrentQueue<(string indexName, IndexingReferenceLoadWarning.WarningDetails warningDetails)> _warningReferenceDocumentLoadsQueue = new();

        private MismatchedReferencesLoadWarning _mismatchedReferencesLoadWarning;

        private readonly ConcurrentQueue<(string indexName, string fieldName)> _warningComplexFieldAutoIndexing = new();

        private readonly HashSet<string> _cpuExhaustionWarningIndexNames = new();
        private bool _isCpuExhaustionWarningAdded = false;

        private Timer _indexingTimer;
        private readonly Logger _logger;
        private readonly object _locker = new();

        internal TimeSpan MinUpdateInterval = TimeSpan.FromSeconds(15);

        public Indexing([NotNull] AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter ?? throw new ArgumentNullException(nameof(notificationCenter));

            _logger = LoggingSource.Instance.GetLogger(notificationCenter.Database, GetType().FullName);
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

        public void AddComplexFieldWarning(string indexName, string fieldName)
        {
            _warningComplexFieldAutoIndexing.Enqueue((indexName, fieldName));
        }

        public void ProcessComplexFields()
        {
            if (_warningComplexFieldAutoIndexing.IsEmpty == false)
                EnsureTimer();
        }

        public void AddWarning(MismatchedReferencesLoadWarning mismatchedReferenceLoadWarningDetails)
        {
            if (CanAdd(out DateTime now) == false)
                return;

            _mismatchedReferencesLoadWarning = mismatchedReferenceLoadWarningDetails;

            EnsureTimer();
        }

        public void AddIndexNameToCpuCreditsExhaustionWarning(string indexName)
        {
            _cpuExhaustionWarningIndexNames.Add(indexName);
        }

        public void RemoveIndexNameFromCpuCreditsExhaustionWarning(string indexName)
        {
            _cpuExhaustionWarningIndexNames.Remove(indexName);
        }

        public void ProcessCpuCreditsExhaustion()
        {
            if (_cpuExhaustionWarningIndexNames.Count > 0)
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
                if (_cpuExhaustionWarningIndexNames.Count == 0)
                {
                    if (_isCpuExhaustionWarningAdded)
                    {
                        _notificationCenter.Dismiss(AlertRaised.GetKey(AlertType.Throttling_CpuCreditsBalance, Source));
                        _isCpuExhaustionWarningAdded = false;
                    }
                }
                else
                {
                    var cpuCreditsExhaustionAlertMessage = new CpuCreditsExhaustionWarning(_cpuExhaustionWarningIndexNames);
                    _notificationCenter.Add(GetCpuCreditsExhaustionAlert(cpuCreditsExhaustionAlertMessage));
                    _isCpuExhaustionWarningAdded = true;
                }

                if (_warningIndexOutputsPerDocumentQueue.IsEmpty && _warningReferenceDocumentLoadsQueue.IsEmpty && _mismatchedReferencesLoadWarning == null && _warningComplexFieldAutoIndexing.IsEmpty)
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

                if (_warningComplexFieldAutoIndexing.IsEmpty == false)
                {
                    var complexFieldAlertMessage = new ComplexFieldsWarning(_warningComplexFieldAutoIndexing);
                    _notificationCenter.Add(GetComplexFieldAlert(complexFieldAlertMessage));
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
            using (_notificationCenter.Storage.Read(HighOutputsRate, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    WarnIndexOutputsPerDocument details;
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                        details = new WarnIndexOutputsPerDocument();
                    else
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<WarnIndexOutputsPerDocument>(detailsJson, HighOutputsRate);

                    return PerformanceHint.Create(_notificationCenter.Database, "High indexing fanout ratio",
                        "Number of map results produced by an index exceeds the performance hint configuration key (MaxIndexOutputsPerDocument).",
                        PerformanceHintType.Indexing, NotificationSeverity.Warning, Source, details);
                }
            }
        }

        private PerformanceHint GetReferenceLoadsPerformanceHint()
        {
            using (_notificationCenter.Storage.Read(ReferencesLoad, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    IndexingReferenceLoadWarning details;
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                        details = new IndexingReferenceLoadWarning();
                    else
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<IndexingReferenceLoadWarning>(detailsJson, ReferencesLoad);

                    return PerformanceHint.Create(_notificationCenter.Database, "High indexing load reference rate",
                        "We have detected high number of LoadDocument() / LoadCompareExchangeValue() calls per single reference item. The update of a reference will result in reindexing all documents that reference it. " +
                        "Please see Indexing Performance graph to check the performance of your indexes.",
                        PerformanceHintType.Indexing_References, NotificationSeverity.Warning, Source, details);
                }
            }
        }

        private AlertRaised GetMismatchedReferencesAlert()
        {
            return AlertRaised.Create(_notificationCenter.Database, $"Loading documents with mismatched collection name in '{_mismatchedReferencesLoadWarning.IndexName}' index",
                "We have detected usage of LoadDocument(doc, collectionName) where loaded document collection is different than given parameter.",
                AlertType.MismatchedReferenceLoad, NotificationSeverity.Warning, Source, _mismatchedReferencesLoadWarning);
        }

        private AlertRaised GetComplexFieldAlert(ComplexFieldsWarning complexFieldsWarning)
        {
            return AlertRaised.Create(_notificationCenter.Database, $"Complex field in Corax auto index", $"We have detected a complex field in an auto index. " +
                    $"To avoid higher resources usage when processing JSON objects, the values of these fields will be replaced with 'JSON_VALUE'. " +
                    $"Please consider querying on individual fields of that object or using a static index. Read more at: https://ravendb.net/l/OB9XW4/6.2", AlertType.Indexing_CoraxComplexItem, NotificationSeverity.Warning, Source, complexFieldsWarning);
        }

        private AlertRaised GetCpuCreditsExhaustionAlert(CpuCreditsExhaustionWarning cpuCreditsExhaustionWarning)
        {
            return AlertRaised.Create(_notificationCenter.Database, "Indexing paused because of CPU credits exhaustion",
                "Indexing has been paused because the CPU credits balance is almost completely used, will be resumed when there are enough CPU credits to use.",
                AlertType.Throttling_CpuCreditsBalance, NotificationSeverity.Warning, Source, cpuCreditsExhaustionWarning);
        }

        public void Dispose()
        {
            _indexingTimer?.Dispose();
        }
    }
}
