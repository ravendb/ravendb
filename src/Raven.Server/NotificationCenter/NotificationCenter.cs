using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Actions;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Collections;
using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter
    {
        private static readonly TimeSpan Infinity = TimeSpan.FromMilliseconds(-1);

        private readonly ActionsStorage _actionsStorage;
        private readonly ConcurrentSet<AsyncQueue<Action>> _watchers = new ConcurrentSet<AsyncQueue<Action>>();
        private readonly AsyncManualResetEvent _postponedNotificationEvent = new AsyncManualResetEvent();
        
        public NotificationCenter(ActionsStorage actionsStorage)
        {
            _actionsStorage = actionsStorage;
        }

        public void Initialize()
        {
            Task.Run(PostponedNotificationsSender);
        }

        public IDisposable TrackActions(AsyncQueue<Action> asyncQueue)
        {
            _watchers.TryAdd(asyncQueue);
            
            return new DisposableAction(() => _watchers.TryRemove(asyncQueue));
        }

        public void Add(Action action)
        {
            if (action.IsPersistent)
            {
                _actionsStorage.Store(action);
            }

            if (_watchers.Count == 0)
                return;

            foreach (var asyncQueue in _watchers)
            {
                asyncQueue.Enqueue(action);
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
            while (true)
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
                            
                            //Add(action.Json);

                            _actionsStorage.ChangePostponeDate(next.Id, null);
                        }
                    }
                }
                catch (Exception e)
                {
                    // TODO arek - log
                    Console.WriteLine(e);
                    throw;
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
    }
}