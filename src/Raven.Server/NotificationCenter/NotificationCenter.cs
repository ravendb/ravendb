using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Server.NotificationCenter.Actions;
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

        public async Task<IDisposable> TrackActions(AsyncQueue<T> asyncQueue, Func<BlittableJsonReaderObject, Task> writeAlertOnLoad)
        {
            _actions.TryAdd(asyncQueue);

            IEnumerable<BlittableJsonReaderObject> existingAlerts;
            using (_alertsStorage.ReadAlerts(out existingAlerts))
            {
                foreach (var alert in existingAlerts)
                {
                    await writeAlertOnLoad(alert);
                }
            }
            
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

        public long GetAlertCount()
        {
            return _alertsStorage.GetAlertCount();
        }
    }
}