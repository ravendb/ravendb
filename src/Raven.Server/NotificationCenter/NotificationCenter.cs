using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Actions;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Json;
using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter
    {
        private readonly ActionsStorage _actionsStorage;

        private readonly ConcurrentSet<AsyncQueue<Action>> _watchers = new ConcurrentSet<AsyncQueue<Action>>();

        public NotificationCenter(ActionsStorage actionsStorage)
        {
            _actionsStorage = actionsStorage;
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

        public IDisposable GetStored(out IEnumerable<BlittableJsonReaderObject> actions, bool excludePostponed = false)
        {
            var scope = _actionsStorage.ReadActionsOrderedByCreationDate(out actions);

            if (excludePostponed)
            {
                var now = SystemTime.UtcNow;

                actions = actions.Where(x =>
                {
                    DateTime postponed;
                    if (ActionsStorage.TryReadDate(x, nameof(Action.PostponedUntil), out postponed) == false)
                        return true;

                    if (postponed > now)
                        return false;

                    return true;
                });
            }

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
        }
    }
}