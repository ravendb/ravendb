using System;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Collections.LockFree;
using Sparrow.Json;

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
        private readonly ConcurrentDictionary<string, SlowWritesDetails.SlowWriteInfo> _slowWrites;

        public SlowWriteNotifications(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string database)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _database = database;
            _slowWrites = new ConcurrentDictionary<string, SlowWritesDetails.SlowWriteInfo>();
        }

        public void Add(string path, double dataSizeInMb, double durationInSec)
        {
            var now = SystemTime.UtcNow;

            if (_slowWrites.TryGetValue(path, out var info))
            {
                if (now - info.Date < UpdateFrequency)
                    return;

                info.DataWrittenInMb = dataSizeInMb;
                info.DurationInSec = durationInSec;
                info.SpeedInMbPerSec = dataSizeInMb / durationInSec;
                info.Date = now;
            }
            else
            {
                info = new SlowWritesDetails.SlowWriteInfo
                {
                    Path = path,
                    DataWrittenInMb = dataSizeInMb,
                    DurationInSec = durationInSec,
                    SpeedInMbPerSec = dataSizeInMb / durationInSec,
                    Date = now
                };
            }

            _slowWrites.AddOrUpdate(path, info, (s, i) => info);

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
            if (_updateNotificationInStorageRequired == false)
                return;

            var hint = GetOrCreateSlowWrites(out var details);

            foreach (var info in _slowWrites)
            {
                details.Writes[info.Key] = info.Value;
            }

            _notificationCenter.Add(hint);

            _updateNotificationInStorageRequired = false;
        }

        internal SlowWritesDetails GetSlowWritesDetails()
        {
            GetOrCreateSlowWrites(out var details);
            return details;
        }

        private PerformanceHint GetOrCreateSlowWrites(out SlowWritesDetails details)
        {
            const string source = "slow-writes";

            var id = PerformanceHint.GetKey(PerformanceHintType.SlowIO, source);

            using (_notificationsStorage.Read(id, out var ntv))
            {
                if (ntv == null || ntv.Json.TryGet(nameof(PerformanceHint.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                {
                    details = new SlowWritesDetails();
                }
                else
                {
                    details = (SlowWritesDetails)EntityToBlittable.ConvertToEntity(
                        typeof(SlowWritesDetails),
                        null,
                        detailsJson,
                        DocumentConventions.Default);
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

        public void Dispose()
        {
            _timer?.Dispose();
        }
    }
}
