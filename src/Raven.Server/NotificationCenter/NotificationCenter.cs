using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Collections.LockFree;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter : IDisposable
    {
        private readonly ConcurrentSet<ConnectedWatcher> _watchers = new ConcurrentSet<ConnectedWatcher>();
        private readonly List<BackgroundWorkBase> _backgroundWorkers = new List<BackgroundWorkBase>();
        private readonly NotificationsStorage _notificationsStorage;
        private readonly object _watchersLock = new object();
        private readonly string _resourceName;
        private readonly CancellationToken _shutdown;
        private PostponedNotificationsSender _postponedNotifications;

        public NotificationCenter(NotificationsStorage notificationsStorage, string resourceName, CancellationToken shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _resourceName = resourceName;
            _shutdown = shutdown;
            Options = new NotificationCenterOptions();
            Paging = new Paging(this, _notificationsStorage);
        }

        public bool IsInitialized { get; set; }

        public void Initialize(DocumentDatabase database = null)
        {
            _postponedNotifications = new PostponedNotificationsSender(_resourceName, _notificationsStorage, _watchers, _shutdown);
            _backgroundWorkers.Add(_postponedNotifications);

            if (database != null)
                _backgroundWorkers.Add(new DatabaseStatsSender(database, this));

            IsInitialized = true;
        }

        public readonly Paging Paging;

        public readonly NotificationCenterOptions Options;

        private void StartBackgroundWorkers()
        {
            foreach (var worker in _backgroundWorkers)
            {
                worker.Start();
            }
        }

        private void StopBackgroundWorkers()
        {
            foreach (var worker in _backgroundWorkers)
            {
                worker.Stop();
            }
        }

        public IDisposable TrackActions(AsyncQueue<Notification> notificationsQueue, IWebsocketWriter webSockerWriter)
        {
            var watcher = new ConnectedWatcher
            {
                NotificationsQueue = notificationsQueue,
                Writer = webSockerWriter
            };

            lock (_watchersLock)
            {
                _watchers.TryAdd(watcher);

                if (_watchers.Count == 1)
                    StartBackgroundWorkers();
            }

            return new DisposableAction(() =>
            {
                lock (_watchersLock)
                {
                    _watchers.TryRemove(watcher);

                    if (_watchers.Count == 0)
                        StopBackgroundWorkers();
                }
            });
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

            // ReSharper disable once InconsistentlySynchronizedField
            foreach (var watcher in _watchers)
            {
                watcher.NotificationsQueue.Enqueue(notification);
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

            _postponedNotifications?.Set();
        }

        public void Dispose()
        {
            Paging?.Dispose();

            foreach (var worker in _backgroundWorkers)
            {
                worker.Dispose();
            }
        }

        public class ConnectedWatcher
        {
            public AsyncQueue<Notification> NotificationsQueue;

            public IWebsocketWriter Writer;
        }
    }
}