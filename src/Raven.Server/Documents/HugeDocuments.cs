using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents
{
    public class HugeDocuments
    {
        private static readonly string PerformanceHintSource = "Documents";
        internal static readonly string HugeDocumentsId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.HugeDocuments}/{PerformanceHintSource}";
        private readonly object _addHintSyncObj = new object();
        private readonly SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long> _hugeDocs;
        private readonly long _maxWarnSize;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        public HugeDocuments(NotificationCenter.NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database, int maxCollectionSize, long maxWarnSize)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _maxWarnSize = maxWarnSize;
            _hugeDocs = new SizeLimitedConcurrentDictionary<Tuple<string, DateTime>, long>(maxCollectionSize);
        }

        public void AddIfDocIsHuge(Document doc)
        {
            if (doc.Data == null)
                return;

            if (doc.Data.Size > _maxWarnSize)
            {
                _hugeDocs.Set(new Tuple<string, DateTime>(doc.Id, DateTime.UtcNow), doc.Data.Size);
                AddHint(doc.Id, doc.Data.Size);
            }
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
                var performanceHint = GetOrCreatePerformanceHint(out var details);
                details.Update(id, size);
                _notificationCenter.Add(performanceHint);
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
                    details = (HugeDocumentsDetails)EntityToBlittable.ConvertToEntity(
                        typeof(HugeDocumentsDetails),
                        HugeDocumentsId,
                        detailsJson,
                        DocumentConventions.Default);
                }

                string message = $"We have detected that some documents has surpassed the configured size threshold ({new Size(_maxWarnSize, SizeUnit.Bytes)}). It might have performance impact. You can alter warning limits by changing '{RavenConfiguration.GetKey(x => x.PerformanceHints.HugeDocumentSize)}' configuration value.";
                

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
    }
}
