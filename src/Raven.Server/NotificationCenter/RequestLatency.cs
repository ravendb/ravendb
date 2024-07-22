using System;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class RequestLatency : IDisposable
    {
        private static readonly string QueryRequestLatenciesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.RequestLatency}/Query";
        private readonly object _addHintSyncObj = new object();
        private readonly Logger _logger;
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;

        private volatile bool _needsSync;
        private PerformanceHint _performanceHint;
        private RequestLatencyDetail _details;

        private Timer _timer;

        public RequestLatency(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
        }

        public void AddHint(long duration, string action, string query)
        {
            lock (_addHintSyncObj)
            {
                if (_performanceHint == null)
                    _performanceHint = GetOrCreatePerformanceLatencies(out _details);

                _details.Update(duration, action, query);
                _needsSync = true;

                if (_timer != null)
                    return;

                _timer = new Timer(UpdateRequestLatency, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }
        }

        internal void UpdateRequestLatency(object state)
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
                    _logger.Info("Error in a request latency timer", e);
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
                using (ntv)
                {
                    if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                    {
                        details = new RequestLatencyDetail();
                    }
                    else
                    {
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<RequestLatencyDetail>(detailsJson, QueryRequestLatenciesId);
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

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
