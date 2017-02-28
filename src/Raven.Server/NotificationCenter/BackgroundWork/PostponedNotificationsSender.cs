using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.BackgroundWork
{
    public class PostponedNotificationsSender
    {
        private static readonly TimeSpan Infinity = TimeSpan.FromMilliseconds(-1);
        
        private readonly NotificationsStorage _notificationsStorage;
        private readonly ConcurrentSet<NotificationCenter.ConnectedWatcher> _watchers;
        private readonly Logger _logger;
        private readonly AsyncManualResetEvent _event;
        private CancellationToken _shutdown;

        public PostponedNotificationsSender(NotificationsStorage notificationsStorage,
            ConcurrentSet<NotificationCenter.ConnectedWatcher> watchers, Logger logger, CancellationToken shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _watchers = watchers;
            _logger = logger;
            _shutdown = shutdown;
            _event = new AsyncManualResetEvent(_shutdown);
        }

        public async Task Run()
        {
            _shutdown.Register(() =>
            {
                _event.Set();
            });

            while (_shutdown.IsCancellationRequested == false)
            {
                try
                {
                    var notifications = GetPostponedNotifications(1, DateTime.MaxValue);

                    TimeSpan wait;
                    if (notifications.Count == 0)
                        wait = Infinity;
                    else
                        wait = notifications.Peek().PostponedUntil - SystemTime.UtcNow;

                    if (wait == Infinity || wait > TimeSpan.Zero)
                        await _event.WaitAsync(wait);

                    if (_shutdown.IsCancellationRequested)
                        break;

                    _event.Reset(true);
                    notifications = GetPostponedNotifications(int.MaxValue, SystemTime.UtcNow);

                    while (notifications.Count > 0)
                    {
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
                catch (OperationCanceledException)
                {
                    // shutdown
                    return;
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Error on sending postponed notification", e);
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
                    if (_shutdown.IsCancellationRequested)
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
            _event.SetByAsyncCompletion();
        }
    }
}