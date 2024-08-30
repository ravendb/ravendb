using System;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public sealed class RequestLatency : IDisposable
    {
        private static readonly string QueryRequestLatenciesId = $"{NotificationType.PerformanceHint}/{PerformanceHintType.RequestLatency}/Query";
        private readonly object _locker = new();
        private readonly Logger _logger;
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        private volatile bool _needsSync;
        private PerformanceHint _performanceHint;
        private RequestLatencyDetail _details;

        private Timer _timer;

        public RequestLatency([NotNull] AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter ?? throw new ArgumentNullException(nameof(notificationCenter));

            _logger = LoggingSource.Instance.GetLogger(notificationCenter.Database, GetType().FullName);
        }

        public void AddHint(long duration, string action, string query)
        {
            lock (_locker)
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

                lock (_locker)
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
            using (_notificationCenter.Storage.Read(QueryRequestLatenciesId, out var ntv))
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
                        _notificationCenter.Database,
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
