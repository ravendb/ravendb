using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Meters;

namespace Raven.Server.NotificationCenter
{
    public class SlowWriteNotifications : IDisposable
    {
        internal TimeSpan UpdateFrequency = TimeSpan.FromSeconds(15);
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;
        private Timer _timer;
        private bool _updateNotificationInStorageRequired;
        private readonly object _pagerCreationLock = new object();
        private readonly ConcurrentDictionary<string, SlowIoDetails.SlowWriteInfo> _slowWrites;
        private readonly Logger _logger;
        private bool _shouldUpdateStorageKeys = true;

        public SlowWriteNotifications(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _slowWrites = new ConcurrentDictionary<string, SlowIoDetails.SlowWriteInfo>();
            _logger = LoggingSource.Instance.GetLogger(database, GetType().FullName);
        }

        public void Add(IoChange ioChange)
        {
            var now = SystemTime.UtcNow;

            if (_slowWrites.TryGetValue(ioChange.Key, out var info))
            {
                if (now - info.Date < UpdateFrequency)
                    return;

                info.Update(ioChange, now);
            }
            else
            {
                info = new SlowIoDetails.SlowWriteInfo(ioChange, now);

                _slowWrites.TryAdd(ioChange.Key, info);
            }

            _updateNotificationInStorageRequired = true;

            if (_timer != null)
                return;

            lock (_pagerCreationLock)
            {
                if (_timer != null)
                    return;

                _timer = new Timer(UpdateNotificationInStorage, null, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1));
            }
        }

        internal void UpdateNotificationInStorage(object state)
        {
            try
            {
                if (_updateNotificationInStorageRequired == false)
                    return;

                var hint = GetOrCreateSlowIoPerformanceHint(out var details);

                foreach (var info in _slowWrites)
                {
                    details.Writes[info.Key] = info.Value;
                }

                if (details.Writes.Count > SlowIoDetails.MaxNumberOfWrites)
                    details.Writes = details.Writes
                        .OrderBy(x => x.Value.Date)
                        .TakeLast(SlowIoDetails.MaxNumberOfWrites)
                        .ToDictionary(x => x.Key, x => x.Value);

                _notificationCenter.Add(hint);

                _updateNotificationInStorageRequired = false;
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in a notification center timer", e);
            }
        }

        internal SlowIoDetails GetSlowIoDetails()
        {
            GetOrCreateSlowIoPerformanceHint(out var details);
            return details;
        }

        private PerformanceHint GetOrCreateSlowIoPerformanceHint(out SlowIoDetails details)
        {
            const string source = "slow-writes";

            var id = PerformanceHint.GetKey(PerformanceHintType.SlowIO, source);

            using (_notificationsStorage.Read(id, out var ntv))
            {
                using (ntv)
                {
                    if (ntv == null ||
                        ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false ||
                        detailsJson == null)
                    {
                        details = new SlowIoDetails();
                        _shouldUpdateStorageKeys = false;
                    }
                    else
                    {
                        details = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<SlowIoDetails>(detailsJson);

                        // Modified the key structure from {Path} to {Type}/{Path} to support the inclusion of all IO metric types, not just JournalWrite entries.
                        UpdateStorageKeysIfNeeded(details);
                    }

                    return PerformanceHint.Create(
                        _database,
                        "An extremely slow write to disk",
                        "We have detected very slow writes",
                        PerformanceHintType.SlowIO,
                        NotificationSeverity.Info,
                        source,
                        details
                    );
                }
            }
        }

        private void UpdateStorageKeysIfNeeded(SlowIoDetails details)
        {
            if (_shouldUpdateStorageKeys == false)
                return;

            var oldWrites = details.Writes;
            details.Writes = new Dictionary<string, SlowIoDetails.SlowWriteInfo>();

            foreach (var oldWriteEntry in oldWrites.Where(kv => kv.Key == kv.Value.Path))
            {
                oldWriteEntry.Value.Type = IoMetrics.MeterType.JournalWrite;
                details.Writes.Add(oldWriteEntry.Value.Key, oldWriteEntry.Value);
            }

            _shouldUpdateStorageKeys = false;
        }

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
