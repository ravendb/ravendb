using System;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class HugeDocuments : IDisposable
    {
        private static readonly string PerformanceHintSource = "Documents";
        internal static readonly string HugeDocumentsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.HugeDocuments}/{PerformanceHintSource}";
        private readonly object _addHintSyncObj = new object();
        private readonly SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long> _hugeDocs;
        private readonly Logger _logger;
        private readonly long _maxWarnSize;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        private volatile bool _needsSync;
        private PerformanceHint _performanceHint;
        private HugeDocumentsDetails _details;

        private Timer _timer;

        public HugeDocuments(NotificationCenter.NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database, int maxCollectionSize, long maxWarnSize)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _maxWarnSize = maxWarnSize;
            _hugeDocs = new SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long>(maxCollectionSize);
            _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
        }

        public void AddIfDocIsHuge(Document doc)
        {
            if (doc.Id == null || doc.Data == null)
                return;

            AddIfDocIsHuge(doc.Id, doc.Data.Size);
        }

        public void AddIfDocIsHuge(string id, int size)
        {
            if (size > _maxWarnSize)
            {
                _hugeDocs.Set(new Tuple<string, DateTime>(id, DateTime.UtcNow), size);
                AddHint(id, size);
            }
        }

        private void AddHint(string id, int size)
        {
            lock (_addHintSyncObj)
            {
                if (_performanceHint == null)
                    _performanceHint = GetOrCreatePerformanceHint(out _details);

                _details.Update(id, size);
                _needsSync = true;

                if (_timer != null)
                    return;

                _timer = new Timer(UpdateHugeDocuments, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
        }

        internal void UpdateHugeDocuments(object state)
        {
            try
            {
                if (_needsSync == false)
                    return;

                lock (_addHintSyncObj)
                {
                    _needsSync = false;

                    _performanceHint.RefreshCreatedAt();
                    _notificationCenter.Add(_performanceHint);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in a huge documents timer", e);
            }
        }

        public SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long> GetHugeDocuments()
        {
            return _hugeDocs;
        }

        private PerformanceHint GetOrCreatePerformanceHint(out HugeDocumentsDetails details)
        {
            //Read() is transactional, so this is thread-safe
            using (_notificationsStorage.Read(HugeDocumentsId, out var ntv))
            {
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                {
                    details = new HugeDocumentsDetails();
                }
                else
                {
                    details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<HugeDocumentsDetails>(detailsJson, HugeDocumentsId);
                }

                string message = $"We have detected that some documents have surpassed the configured threshold size ({new Size(_maxWarnSize, SizeUnit.Bytes)}). " +
                                 "This may cause a performance impact. " +
                                 $"You can alter the warning limits by changing '{RavenConfiguration.GetKey(x => x.PerformanceHints.HugeDocumentSize)}' configuration value.";

                return PerformanceHint.Create(
                    _database,
                    "Huge documents",
                    message,
                    PerformanceHintType.HugeDocuments,
                    NotificationSeverity.Warning,
                    PerformanceHintSource,
                    details
                );
            }
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
