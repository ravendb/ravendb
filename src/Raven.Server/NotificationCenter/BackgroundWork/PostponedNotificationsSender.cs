using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public class PostponedNotificationsSender : BackgroundWorkBase
    {
        private static readonly TimeSpan Infinity = TimeSpan.FromMilliseconds(-1);
        
        private readonly NotificationsStorage _notificationsStorage;
        private readonly ConcurrentSet<NotificationCenter.ConnectedWatcher> _watchers;
        private AsyncManualResetEvent _event;

        public PostponedNotificationsSender(string resourceName, NotificationsStorage notificationsStorage,
            ConcurrentSet<NotificationCenter.ConnectedWatcher> watchers, CancellationToken shutdown)
            : base(resourceName, shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _watchers = watchers;
        }

        protected override void InitializeWork()
        {
            _event = new AsyncManualResetEvent(CancellationToken);

            CancellationToken.Register(() =>
            {
                _event.Set();
            });
        }

        protected override async Task DoWork()
        {
            var notifications = GetPostponedNotifications(1, DateTime.MaxValue);

            TimeSpan wait;
            if (notifications.Count == 0)
                wait = Infinity;
            else
                wait = notifications.Peek().PostponedUntil - SystemTime.UtcNow;

            if (wait == Infinity || wait > TimeSpan.Zero)
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

                NotificationTableValue notification;
                using (_notificationsStorage.Read(next.Id, out notification))
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

        private Queue<PostponedNotification> GetPostponedNotifications(int take, DateTime cutoff)
        {
            var next = new Queue<PostponedNotification>();

            IEnumerable<NotificationTableValue> actions;
            using (_notificationsStorage.ReadPostponedActions(out actions, cutoff))
            {
                foreach (var action in actions)
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

            return next;
        }

        private class PostponedNotification
        {
            public DateTime PostponedUntil;

            public string Id;
        }

        public void Set()
        {
            _event?.SetByAsyncCompletion();
        }
    }
}