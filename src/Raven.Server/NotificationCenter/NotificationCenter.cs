using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Collections;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter : NotificationsBase, IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<NotificationCenter>("Server");
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _database;
        private readonly CancellationToken _shutdown;
        private PostponedNotificationsSender _postponedNotificationSender;

        public NotificationCenter(NotificationsStorage notificationsStorage, string database, CancellationToken shutdown, RavenConfiguration config)
        {
            _notificationsStorage = notificationsStorage;
            _database = database;
            _shutdown = shutdown;
            _config = config;
            Options = new NotificationCenterOptions();
            Paging = new Paging(this, _notificationsStorage, database);
            RequestLatency = new RequestLatency(this, _notificationsStorage, database);
            EtlNotifications = new EtlNotifications(this, _notificationsStorage, _database);
            SlowWrites = new SlowWriteNotifications(this, _notificationsStorage, _database);
            OutOfMemory = new OutOfMemoryNotifications(this);
        }


        public bool IsInitialized { get; set; }

        public void Initialize(DocumentDatabase database = null)
        {
            _postponedNotificationSender = new PostponedNotificationsSender(_database, _notificationsStorage, Watchers, _shutdown);
            BackgroundWorkers.Add(_postponedNotificationSender);

            if (database != null)
                BackgroundWorkers.Add(new DatabaseStatsSender(database, this));

            IsInitialized = true;
        }

        public readonly Paging Paging;
        public readonly RequestLatency RequestLatency;
        public readonly EtlNotifications EtlNotifications;
        public readonly SlowWriteNotifications SlowWrites;
        public readonly OutOfMemoryNotifications OutOfMemory;
        
        public readonly NotificationCenterOptions Options;
        private readonly RavenConfiguration _config;

        public void Add(Notification notification, DateTime? postponeUntil = null, bool updateExisting = true)
        {
            try
            {
                if (_config.Notifications.ShouldFilterOut(notification))
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Filtered out notification. Id: '{notification.Id}', Title: '{notification.Title}', message: '{notification.Message}'");
                    return;
                }

                if (notification.IsPersistent)
                {
                    try
                    {
                        if (_notificationsStorage.Store(notification, postponeUntil, updateExisting) == false)
                            return;
                    }
                    catch (Exception e)
                    {
                        // if we fail to save the persistent notification in the storage,
                        // (OOME or any other storage error)
                        // we still want to send it to any of the connected watchers
                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Failed to save a persistent notification '{notification.Id}' " +
                                        $"to the notification center. " +
                                        $"Title: {notification.Title}, message: {notification.Message}", e);
                    }
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
                    if (watcher.Filter != null && watcher.Filter(notification.Database) == false)
                    {
                        continue;
                    }
                    
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

        public long GetPerformanceHintCount()
        {
            return _notificationsStorage.GetPerformanceHintCount();
        }

        public void Dismiss(string id, RavenTransaction existingTransaction = null, bool sendNotificationEvenIfDoesntExist = true)
        {
            var deleted = _notificationsStorage.Delete(id, existingTransaction);
            if (deleted == false && sendNotificationEvenIfDoesntExist == false)
                return;

            // send this notification even when notification doesn't exist 
            // we don't persist all notifications
            Add(NotificationUpdated.Create(id, NotificationUpdateType.Dismissed));
        }

        public string GetDatabaseFor(string id) => _notificationsStorage.GetDatabaseFor(id);

        public void Postpone(string id, DateTime until)
        {
            _notificationsStorage.ChangePostponeDate(id, until);

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Postponed));

            _postponedNotificationSender?.Set();
        }

        public new void Dispose()
        {
            Paging?.Dispose();
            RequestLatency?.Dispose();
            SlowWrites?.Dispose();

            base.Dispose();
        }
    }

    public class ConnectedWatcher
    {
        public AsyncQueue<DynamicJsonValue> NotificationsQueue;

        public IWebsocketWriter Writer;

        public Func<string, bool> Filter;
    }
}
