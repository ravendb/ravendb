using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter
    {
        private readonly NotificationsStorage _notificationsStorage;
        private readonly CancellationToken _shutdown;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers = new ConcurrentSet<ConnectedWatcher>();
        private readonly PostponedNotificationsSender _postponedNotifications;

        private readonly Logger Logger;

        public NotificationCenter(NotificationsStorage notificationsStorage, string resourceName, CancellationToken shutdown)
        {
            _notificationsStorage = notificationsStorage;
            _shutdown = shutdown;

            Logger = LoggingSource.Instance.GetLogger<NotificationCenter>(resourceName);
            _postponedNotifications = new PostponedNotificationsSender(_notificationsStorage, _watchers, Logger, _shutdown);
        }

        public void Initialize(DocumentDatabase database = null)
        {
            Task.Run(_postponedNotifications.Run, _shutdown);

            if (database != null)
                Task.Run(new DatabaseStatsSender(database, this, Logger).Run, _shutdown);
        }

        public int NumberOfWatchers => _watchers.Count;

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

            _postponedNotifications.Set();
        }
        
        public class ConnectedWatcher
        {
            public AsyncQueue<Notification> NotificationsQueue;

            public IWebsocketWriter Writer;
        }
    }
}