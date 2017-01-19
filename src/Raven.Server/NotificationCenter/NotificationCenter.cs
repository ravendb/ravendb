using System;
using System.Collections.Generic;
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

        public IDisposable GetStored(out IEnumerable<BlittableJsonReaderObject> actions)
        {
            return _actionsStorage.ReadActions(out actions);
        }

        public long GetAlertCount()
        {
            return _actionsStorage.GetAlertCount();
        }

        public void Delete(string id)
        {
            var deleted = _actionsStorage.Delete(id);

            if (deleted == false)
                return;

            Add(NotificationDeleted.Create(id));
        }

        public void DismissUntil(string id, DateTime until)
        {
            _actionsStorage.ChangeDismissUntilDate(id, until);

            Add(NotificationDismissed.Create(id, until));
        }
    }
}