using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Dashboard;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
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
            ConflictRevisionsExceeded = new ConflictRevisionsExceeded(this, _notificationsStorage, database);
            TombstoneNotifications = new TombstoneNotifications(this, _notificationsStorage, database);
            Indexing = new Indexing(this, _notificationsStorage, database);
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
            
            _initializeTaskSource.SetResult(this);
        }

        private readonly TaskCompletionSource<NotificationCenter> _initializeTaskSource = new TaskCompletionSource<NotificationCenter>(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<NotificationCenter> InitializeTask => _initializeTaskSource.Task;
        
        public readonly Paging Paging;
        public readonly ConflictRevisionsExceeded ConflictRevisionsExceeded;
        public readonly TombstoneNotifications TombstoneNotifications;
        public readonly Indexing Indexing;
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

                if (Watchers.IsEmpty)
                    return;

                using (_notificationsStorage.Read(notification.Id, out NotificationTableValue existing))
                {
                    using (existing)
                    {
                        if (existing?.PostponedUntil > SystemTime.UtcNow)
                            return;
                    }
                }

                foreach (var watcher in Watchers)
                {
                    if (watcher.Filter != null && watcher.Filter(notification.Database, false) == false)
                    {
                        continue;
                    }

                    // serialize to avoid race conditions
                    // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                    watcher.Enqueue(notification.ToJson());
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

            actions = Filter(actions);

            return scope;

            static IEnumerable<NotificationTableValue> Filter(IEnumerable<NotificationTableValue> actions)
            {
                var now = SystemTime.UtcNow;

                foreach (var ntv in actions)
                {
                    if (ntv.PostponedUntil == null)
                    {
                        yield return ntv;
                        continue;
                    }

                    if (ntv.PostponedUntil <= now)
                    {
                        yield return ntv;
                        continue;
                    }

                    ntv.Dispose();
                }
            }
        }

        public string GetStoredMessage(string id)
        {
            using (_notificationsStorage.Read(id, out var value))
            {
                using (value)
                {
                    value.Json.TryGet(nameof(Notification.Message), out string message);
                    return message;
                }
            }
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

        public bool Exists(string id)
        {
            return _notificationsStorage.Exists(id);
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
            ConflictRevisionsExceeded?.Dispose();
            Indexing?.Dispose();
            RequestLatency?.Dispose();
            SlowWrites?.Dispose();

            base.Dispose();
        }
    }

    public sealed class ConnectedWatcher
    {
        private readonly AsyncQueue<DynamicJsonValue> _notificationsQueue;
        private readonly int _maxNotificationsQueueSize;

        public readonly IWebsocketWriter Writer;

        public readonly CanAccessDatabase Filter;

        public ConnectedWatcher(AsyncQueue<DynamicJsonValue> notificationsQueue, int maxNotificationsQueueSize, IWebsocketWriter writer, CanAccessDatabase filter)
        {
            _notificationsQueue = notificationsQueue;
            _maxNotificationsQueueSize = maxNotificationsQueueSize;
            Writer = writer;
            Filter = filter;
        }

        public void Enqueue(DynamicJsonValue json)
        {
            if (_notificationsQueue.Count >= _maxNotificationsQueueSize) 
                return;

            _notificationsQueue.Enqueue(json);
        }
    }
}
