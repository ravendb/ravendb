using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter : NotificationsBase, IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<NotificationCenter>("NotificationCenter");
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _resourceName;
        private readonly CancellationToken _shutdown;
        private PostponedNotificationsSender _postponedNotificationSender;

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
            _postponedNotificationSender = new PostponedNotificationsSender(_resourceName, _notificationsStorage, Watchers, _shutdown);
            BackgroundWorkers.Add(_postponedNotificationSender);

            if (database != null)
                BackgroundWorkers.Add(new DatabaseStatsSender(database, this));

            IsInitialized = true;
        }

        public readonly Paging Paging;

        public readonly NotificationCenterOptions Options;

        public void Add(Notification notification)
        {
            try
            {
                if (notification.IsPersistent)
                {
                    if (_notificationsStorage.Store(notification) == false)
                        return;
                }

                if (Watchers.Count == 0)
                    return;

                using (_notificationsStorage.Read(notification.Id, out NotificationTableValue existing))
                {
                    if (existing?.PostponedUntil > SystemTime.UtcNow)
                        return;
                }
                
                foreach (var watcher in Watchers)
                {
                    // serialize to avoid race conditions
                    // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                    watcher.NotificationsQueue.Enqueue(notification.ToJson());
                }
            }
            catch (ObjectDisposedException)
            {
                // we are disposing
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to add notification '{notification.Id}' to the notification center. Title: {notification.Title}, message: {notification.Message}", e);
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
            _notificationsStorage.Delete(id);

            // send this notificaton even when notification doesn't exist 
            // we don't persist all notifications
            Add(NotificationUpdated.Create(id, NotificationUpdateType.Dismissed));
        }

        public void Postpone(string id, DateTime until)
        {
            _notificationsStorage.ChangePostponeDate(id, until);

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Postponed));

            _postponedNotificationSender?.Set();
        }

        public new void Dispose()
        {
            Paging?.Dispose();

            base.Dispose();
        }
    }

    public class ConnectedWatcher
    {
        public AsyncQueue<DynamicJsonValue> NotificationsQueue;

        public IWebsocketWriter Writer;
    }
}
