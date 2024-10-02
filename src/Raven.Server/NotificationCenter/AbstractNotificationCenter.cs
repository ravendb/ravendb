using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter;

public abstract class AbstractNotificationCenter : NotificationsBase
{
    private readonly Logger _logger;

    public readonly NotificationsStorage Storage;

    private readonly TaskCompletionSource<AbstractNotificationCenter> _initializeTaskSource = new TaskCompletionSource<AbstractNotificationCenter>(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task<AbstractNotificationCenter> InitializeTask => _initializeTaskSource.Task;

    protected AbstractNotificationCenter(
        [NotNull] NotificationsStorage storage,
        [NotNull] RavenConfiguration configuration,
        [NotNull] Logger logger)
    {
        Storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        Options = new NotificationCenterOptions();
        OutOfMemory = new OutOfMemoryNotifications(this);
    }

    public bool IsInitialized { get; set; }

    protected abstract PostponedNotificationsSender PostponedNotificationSender { get; }

    public virtual void Initialize()
    {
        BackgroundWorkers.Add(PostponedNotificationSender);

        IsInitialized = true;

        _initializeTaskSource.SetResult(this);
    }

    public readonly OutOfMemoryNotifications OutOfMemory;

    public readonly NotificationCenterOptions Options;
    private readonly RavenConfiguration _configuration;

    public void Add(Notification notification, DateTime? postponeUntil = null, bool updateExisting = true)
    {
        try
        {
            if (_configuration.Notifications.ShouldFilterOut(notification))
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Filtered out notification. Id: '{notification.Id}', Title: '{notification.Title}', message: '{notification.Message}'");
                return;
            }

            if (notification.IsPersistent)
            {
                try
                {
                    if (Storage.Store(notification, postponeUntil, updateExisting) == false)
                        return;
                }
                catch (Exception e)
                {
                    // if we fail to save the persistent notification in the storage,
                    // (OOME or any other storage error)
                    // we still want to send it to any of the connected watchers
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Failed to save a persistent notification '{notification.Id}' " +
                                     $"to the notification center. " +
                                     $"Title: {notification.Title}, message: {notification.Message}", e);
                }
            }

            if (Watchers.IsEmpty)
                return;

            using (Storage.Read(notification.Id, out NotificationTableValue existing))
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
            if (_logger.IsInfoEnabled)
                _logger.Info($"Failed to add notification '{notification.Id}' to the notification center. Title: {notification.Title}, message: {notification.Message}", e);
        }
    }

    public IDisposable GetStored(out IEnumerable<NotificationTableValue> actions, bool postponed = true)
    {
        var scope = Storage.ReadActionsOrderedByCreationDate(out actions);

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
        using (Storage.Read(id, out var value))
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
        return Storage.GetAlertCount();
    }

    public long GetPerformanceHintCount()
    {
        return Storage.GetPerformanceHintCount();
    }

    public void Dismiss(string id, RavenTransaction existingTransaction = null, bool sendNotificationEvenIfDoesntExist = true)
    {
        var deleted = Storage.Delete(id, existingTransaction);
        if (deleted == false && sendNotificationEvenIfDoesntExist == false)
            return;

        // send this notification even when notification doesn't exist
        // we don't persist all notifications
        Add(NotificationUpdated.Create(id, NotificationUpdateType.Dismissed));
    }

    public bool Exists(string id)
    {
        return Storage.Exists(id);
    }

    public string GetDatabaseFor(string id) => Storage.GetDatabaseFor(id);

    public void Postpone(string id, DateTime until)
    {
        Storage.ChangePostponeDate(id, until);

        Add(NotificationUpdated.Create(id, NotificationUpdateType.Postponed));

        PostponedNotificationSender?.Set();
    }
}
