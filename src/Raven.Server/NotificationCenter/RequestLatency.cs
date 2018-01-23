using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class RequestLatency
    {
        private static readonly string QueryRequestLatenciesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.RequestLatency}/Query";
        private readonly object _addHintSyncObj = new object();
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        public RequestLatency(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
        }

        public void AddHint(long duration, string action, string query)
        {
            lock (_addHintSyncObj)
            {
                var requestLatencyPerformanceHint = GetOrCreatePerformanceLatencies(out var details);
                details.Update(duration, action, query);
                _notificationCenter.Add(requestLatencyPerformanceHint);
            }
        }

        public RequestLatencyDetail GetRequestLatencyDetails()
        {
            GetOrCreatePerformanceLatencies(out var details);
            return details;
        }

        private PerformanceHint GetOrCreatePerformanceLatencies(out RequestLatencyDetail details)
        {
            //Read() is transactional, so this is thread-safe
            using (_notificationsStorage.Read(QueryRequestLatenciesId, out var ntv))
            {
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                {
                    details = new RequestLatencyDetail();
                }
                else
                {
                    details = (RequestLatencyDetail)EntityToBlittable.ConvertToEntity(
                        typeof(RequestLatencyDetail),
                        QueryRequestLatenciesId,
                        detailsJson,
                        DocumentConventions.Default);
                }

                return PerformanceHint.Create(
                    _database,
                    "Request latency is too high",
                    "We have detected that some query duration has surpassed the configured threshold",
                    PerformanceHintType.RequestLatency,
                    NotificationSeverity.Warning,
                    "Query",
                    details
                );
            }
        }
    }
}
