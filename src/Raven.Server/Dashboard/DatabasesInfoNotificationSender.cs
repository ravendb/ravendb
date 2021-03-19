using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Sparrow.Collections;

namespace Raven.Server.Dashboard
{
    public class DatabasesInfoNotificationSender : BackgroundWorkBase
    {
        private readonly ServerStore _serverStore;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;
        private DateTime _lastSentNotification = DateTime.MinValue;

        public DatabasesInfoNotificationSender(string resourceName, ServerStore serverStore,
            ConcurrentSet<ConnectedWatcher> watchers, TimeSpan notificationsThrottle, CancellationToken shutdown)
            : base(resourceName, shutdown)
        {
            _serverStore = serverStore;
            _watchers = watchers;
            _notificationsThrottle = notificationsThrottle;
        }

        protected override async Task DoWork()
        {
            var now = SystemTime.UtcNow;
            var timeSpan = now - _lastSentNotification;
            if (timeSpan < _notificationsThrottle)
            {
                await WaitOrThrowOperationCanceled(_notificationsThrottle - timeSpan);
            }

            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                if (_watchers.Count == 0)
                    return;

                var databasesInfo = new List<AbstractDashboardNotification>();

                foreach (var item in DatabasesInfoRetriever.FetchDatabasesInfo(_serverStore, null, Cts.Token))
                {
                    databasesInfo.Add(item);
                }

                foreach (var watcher in _watchers)
                {
                    foreach (var info in databasesInfo)
                    {
                        if (watcher.Filter != null)
                        {
                            var asJson = info.ToJsonWithFilter(watcher.Filter);
                            if (asJson != null)
                            {
                                watcher.NotificationsQueue.Enqueue(asJson);
                            }
                        }
                        else
                        {
                            // serialize to avoid race conditions
                            // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                            watcher.NotificationsQueue.Enqueue(info.ToJson());
                        }
                    }
                }
            }
            finally
            {
                _lastSentNotification = SystemTime.UtcNow;
            }
        }
    }
}
