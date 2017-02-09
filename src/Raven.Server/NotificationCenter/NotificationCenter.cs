using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter
    {
        private static readonly TimeSpan Infinity = TimeSpan.FromMilliseconds(-1);

        private readonly Logger Logger;

        private readonly NotificationsStorage _notificationsStorage;
        private CancellationToken _shutdown;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers = new ConcurrentSet<ConnectedWatcher>();
        private readonly AsyncManualResetEvent _postponedNotificationEvent;
        
        public NotificationCenter(NotificationsStorage notificationsStorage, string resourceName, CancellationToken shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _shutdown = shutdown;
            Logger = LoggingSource.Instance.GetLogger<NotificationsStorage>(resourceName);
            _postponedNotificationEvent = new AsyncManualResetEvent(shutdown);
        }

        public void Initialize()
        {
            Task.Run(PostponedNotificationsSender, _shutdown);
        }

        public IDisposable TrackActions(AsyncQueue<Notification> notificationsQueue, IWebsocketWriter webSockerWriter)
        {
            var watcher = new ConnectedWatcher
            {
                NotificationsQueue = notificationsQueue,
                Writer = webSockerWriter
            };

            _watchers.TryAdd(watcher);
            
            return new DisposableAction(() => _watchers.TryRemove(watcher));
        }

        public void Add(Notification notification)
        {
            if (notification.IsPersistent)
            {
                if (_notificationsStorage.Store(notification) == false)
                    return;
            }

            if (_watchers.Count == 0)
                return;

            NotificationTableValue existing;
            using (_notificationsStorage.Read(notification.Id, out existing))
            {
                if (existing?.PostponedUntil > SystemTime.UtcNow)
                    return;
            }

            foreach (var watcher in _watchers)
            {
                try
                {
                    watcher.NotificationsQueue.Enqueue(notification);
                }
                catch (Exception ex)
                {
                    Logger.Operations($"Failed to perform notification of type {notification.Type}",ex);
                }
            }
        }

        public void AddAfterTransactionCommit(Notification notification, RavenTransaction tx)
        {
            var llt = tx.InnerTransaction.LowLevelTransaction;

            llt.OnDispose += _ =>
            {
                if (llt.Committed == false)
                    return;

                Add(notification);
            };
        }

        public IDisposable GetStored(out IEnumerable<NotificationTableValue> actions, bool postponed = true)
        {
            var scope = _notificationsStorage.ReadActionsOrderedByCreationDate(out actions);

            if (postponed)
                return scope;

            var now = SystemTime.UtcNow;

            actions = actions.Where(x => x.PostponedUntil == null || x.PostponedUntil <= now);

            return scope;
        }

        public long GetAlertCount()
        {
            return _notificationsStorage.GetAlertCount();
        }

        public void Dismiss(string id)
        {
            var deleted = _notificationsStorage.Delete(id);

            if (deleted == false)
                return;

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Dismissed));
        }

        public void Postpone(string id, DateTime until)
        {
            _notificationsStorage.ChangePostponeDate(id, until);

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Postponed));

            _postponedNotificationEvent.SetByAsyncCompletion();
        }

        private async Task PostponedNotificationsSender()
        {
            _shutdown.Register(() =>
            {
                _postponedNotificationEvent.Set();
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
                        await _postponedNotificationEvent.WaitAsync(wait);

                    if (_shutdown.IsCancellationRequested)
                        break;

                    _postponedNotificationEvent.Reset(true);
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
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Error on sending postponed notification", e);
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

        private class ConnectedWatcher
        {
            public AsyncQueue<Notification> NotificationsQueue;

            public IWebsocketWriter Writer;
        }

    }
}