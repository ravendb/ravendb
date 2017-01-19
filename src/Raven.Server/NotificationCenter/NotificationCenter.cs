using System;
using System.Collections.Generic;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Actions;
using Raven.Server.NotificationCenter.Actions.Database;
using Raven.Server.NotificationCenter.Alerts;
using Raven.Server.ServerWide;
using Sparrow.Collections;
using Sparrow.Json;
using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenter<T>  where T : Action
    {
        private readonly AlertsStorage _alertsStorage;

        private readonly ConcurrentSet<AsyncQueue<T>> _actions = new ConcurrentSet<AsyncQueue<T>>();

        public NotificationCenter(AlertsStorage alertsStorage)
        {
            _alertsStorage = alertsStorage;
        }

        public IDisposable TrackActions(AsyncQueue<T> asyncQueue)
        {
            _actions.TryAdd(asyncQueue);
            
            return new DisposableAction(() => _actions.TryRemove(asyncQueue));
        }

        public void Add(T action)
        {
            if (action.Type == ActionType.Alert)
            {
                var alert = action as IAlert;

                if (alert == null)
                    throw new InvalidOperationException($"Action having {action.Type} defined does not implement {nameof(IAlert)} interface");

                _alertsStorage.AddAlert(alert);
            }

            if (_actions.Count == 0)
                return;

            foreach (var asyncQueue in _actions)
            {
                asyncQueue.Enqueue(action);
            }
        }

        public void AddAfterTransactionCommit(T action, RavenTransaction tx)
        {
            var llt = tx.InnerTransaction.LowLevelTransaction;

            llt.OnDispose += _ =>
            {
                if (llt.Committed == false)
                    return;

                Add(action);
            };
        }

        public IDisposable GetAlerts(out IEnumerable<BlittableJsonReaderObject> alerts)
        {
            return _alertsStorage.ReadAlerts(out alerts);
        }

        public long GetAlertCount()
        {
            return _alertsStorage.GetAlertCount();
        }

        public void DeleteAlert(DatabaseAlertType type, string key)
        {
            _alertsStorage.DeleteAlert(AlertUtil.CreateId(type, key));
        }

        public void DeleteAlert(ServerAlertType type, string key)
        {
            _alertsStorage.DeleteAlert(AlertUtil.CreateId(type, key));
        }
    }
}