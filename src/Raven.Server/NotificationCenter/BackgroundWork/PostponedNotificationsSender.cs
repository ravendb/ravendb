using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public sealed class PostponedNotificationsSender : BackgroundWorkBase
    {
        private readonly NotificationsStorage _notificationsStorage;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private AsyncManualResetEvent _event;

        public PostponedNotificationsSender(string resourceName, NotificationsStorage notificationsStorage,
            ConcurrentSet<ConnectedWatcher> watchers, RavenLogger logger, CancellationToken shutdown)
            : base(resourceName, logger, shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _watchers = watchers;
        }

        protected override void InitializeWork()
        {
            _event = new AsyncManualResetEvent(CancellationToken);
        }

        public override void Dispose()
        {
            base.Dispose();
            _event?.Dispose();
        }

        protected override async Task DoWork()
        {
            var notifications = GetPostponedNotifications(1, DateTime.MaxValue);

            TimeSpan wait;
            if (notifications.Count == 0)
                wait = Timeout.InfiniteTimeSpan;
            else
                wait = notifications.Peek().PostponedUntil - SystemTime.UtcNow;

            if (wait == Timeout.InfiniteTimeSpan || wait > TimeSpan.Zero)
                await _event.WaitAsync(wait);

            if (CancellationToken.IsCancellationRequested)
                return;

            _event.Reset(true);
            notifications = GetPostponedNotifications(int.MaxValue, SystemTime.UtcNow);

            while (notifications.Count > 0)
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                var next = notifications.Dequeue();

                using (_notificationsStorage.Read(next.Id, out NotificationTableValue notification))
                {
                    using (notification)
                    {
                        if (notification == null) // could be deleted meanwhile
                            continue;

                        try
                        {
                            foreach (var watcher in _watchers)
                            {
                                await watcher.Writer.WriteToWebSocket(notification.Json);
                            }
                        }
                        finally
                        {
                            _notificationsStorage.ChangePostponeDate(next.Id, null);
                        }
                    }
                }
            }
        }

        private Queue<PostponedNotification> GetPostponedNotifications(int take, DateTime cutoff)
        {
            var next = new Queue<PostponedNotification>();

            using (_notificationsStorage.ReadPostponedActions(out IEnumerable<NotificationTableValue> actions, cutoff))
            {
                foreach (var action in actions)
                {
                    using (action)
                    {
                        if (CancellationToken.IsCancellationRequested)
                            break;

                        next.Enqueue(new PostponedNotification
                        {
                            Id = action.Json[nameof(Notification.Id)].ToString(),
                            PostponedUntil = action.PostponedUntil.Value
                        });

                        if (next.Count == take)
                            break;
                    }
                }
            }

            return next;
        }

        private sealed class PostponedNotification
        {
            public DateTime PostponedUntil;

            public string Id;
        }

        public void Set()
        {
            _event?.Set();
        }
    }
}
