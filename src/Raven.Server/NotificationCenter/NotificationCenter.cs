using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Actions;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;
using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter
    {
        private static readonly TimeSpan Infinity = TimeSpan.FromMilliseconds(-1);

        private readonly Logger Logger;

        private readonly ActionsStorage _actionsStorage;
        private readonly CancellationToken _shutdown;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers = new ConcurrentSet<ConnectedWatcher>();
        private readonly AsyncManualResetEvent _postponedNotificationEvent;
        
        public NotificationCenter(ActionsStorage actionsStorage, string resourceName, CancellationToken shutdown)
        {
            _actionsStorage = actionsStorage;
            _shutdown = shutdown;
            Logger = LoggingSource.Instance.GetLogger<ActionsStorage>(resourceName);
            _postponedNotificationEvent = new AsyncManualResetEvent(shutdown);
        }

        public void Initialize()
        {
            Task.Run(PostponedNotificationsSender);
        }

        public IDisposable TrackActions(AsyncQueue<Action> actionsQueue, IWebsocketWriter webSockerWriter)
        {
            var watcher = new ConnectedWatcher
            {
                ActionsQueue = actionsQueue,
                Writer = webSockerWriter
            };

            _watchers.TryAdd(watcher);
            
            return new DisposableAction(() => _watchers.TryRemove(watcher));
        }

        public void Add(Action action)
        {
            if (action.IsPersistent)
            {
                _actionsStorage.Store(action);
            }

            if (_watchers.Count == 0)
                return;

            ActionTableValue existing;
            using (_actionsStorage.Read(action.Id, out existing))
            {
                if (existing?.PostponedUntil > SystemTime.UtcNow)
                    return;
            }

            foreach (var watcher in _watchers)
            {
                watcher.ActionsQueue.Enqueue(action);
            }
        }

        public void AddAfterTransactionCommit(Action action, RavenTransaction tx)
        {
            var llt = tx.InnerTransaction.LowLevelTransaction;

            llt.OnDispose += _ =>
            {
                if (llt.Committed == false)
                    return;

                Add(action);
            };
        }

        public IDisposable GetStored(out IEnumerable<ActionTableValue> actions, bool postponed = true)
        {
            var scope = _actionsStorage.ReadActionsOrderedByCreationDate(out actions);

            if (postponed)
                return scope;

            var now = SystemTime.UtcNow;

            actions = actions.Where(x => x.PostponedUntil == null || x.PostponedUntil <= now);

            return scope;
        }

        public long GetAlertCount()
        {
            return _actionsStorage.GetAlertCount();
        }

        public void Dismiss(string id)
        {
            var deleted = _actionsStorage.Delete(id);

            if (deleted == false)
                return;

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Dismissed));
        }

        public void Postpone(string id, DateTime until)
        {
            _actionsStorage.ChangePostponeDate(id, until);

            Add(NotificationUpdated.Create(id, NotificationUpdateType.Postponed));

            _postponedNotificationEvent.SetByAsyncCompletion();
        }

        private async Task PostponedNotificationsSender()
        {
            while (_shutdown.IsCancellationRequested == false)
            {
                try
                {
                    var notifications = GetPostponedNotifications();

                    TimeSpan wait;

                    if (notifications.Count == 0)
                        wait = Infinity;
                    else
                        wait = notifications.Peek().PostponedUntil - SystemTime.UtcNow;

                    if (wait == Infinity || wait > TimeSpan.Zero)
                        await _postponedNotificationEvent.WaitAsync(wait);

                    while (notifications.Count > 0)
                    {
                        var next = notifications.Dequeue();

                        ActionTableValue action;
                        using (_actionsStorage.Read(next.Id, out action))
                        {
                            if (action == null) // could be deleted meanwhile
                                continue;

                            try
                            {
                                foreach (var watcher in _watchers)
                                {
                                    await watcher.Writer.WriteToWebSocket(action.Json);
                                }
                            }
                            finally
                            {
                                _actionsStorage.ChangePostponeDate(next.Id, null);
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

        private Queue<PostponedNotification> GetPostponedNotifications()
        {
            var next = new Queue<PostponedNotification>();

            IEnumerable<ActionTableValue> actions;
            using (_actionsStorage.ReadPostponedActions(out actions, SystemTime.UtcNow))
            {
                foreach (var action in actions)
                {
                    next.Enqueue(new PostponedNotification
                    {
                        Id = action.Json[nameof(Action.Id)].ToString(),
                        PostponedUntil = action.PostponedUntil.Value
                    });
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
            public AsyncQueue<Action> ActionsQueue;

            public IWebsocketWriter Writer;
        }

    }
}