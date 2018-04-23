using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Background;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.NotificationCenter
{
    public abstract class NotificationsBase : IDisposable
    {
        private readonly object _watchersLock = new object();

        private TaskCompletionSource<object> _newWebSocket = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        private TaskCompletionSource<object> _allWebSocketsRemoved = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WaitForAllRemoved => _allWebSocketsRemoved.Task;

        protected ConcurrentSet<ConnectedWatcher> Watchers { get; }
        protected List<BackgroundWorkBase> BackgroundWorkers { get; }

        private int _websocketClients;

        public Task WaitForNew()
        {
            lock (_watchersLock)
            {
                return _websocketClients > 0 ? Task.CompletedTask : _newWebSocket.Task;
            }
        }

        protected NotificationsBase()
        {
            Watchers = new ConcurrentSet<ConnectedWatcher>();
            BackgroundWorkers = new List<BackgroundWorkBase>();
        }

        public IDisposable TrackActions(AsyncQueue<DynamicJsonValue> notificationsQueue, IWebsocketWriter webSocketWriter)
        {
            var watcher = new ConnectedWatcher
            {
                NotificationsQueue = notificationsQueue,
                Writer = webSocketWriter
            };

            lock (_watchersLock)
            {
                Watchers.TryAdd(watcher);

                if (Watchers.Count == 1)
                {
                    StartBackgroundWorkers();
                }

                if (watcher.Writer is NotificationCenterWebSocketWriter)
                {
                    if (_websocketClients == 0)
                    {
                        TaskExecutor.CompleteAndReplace(ref _newWebSocket);
                    }
                    _websocketClients++;
                }
            }

            return new DisposableAction(() =>
            {
                lock (_watchersLock)
                {
                    Watchers.TryRemove(watcher);

                    if (Watchers.Count == 0)
                    {
                        StopBackgroundWorkers();
                    }

                    if (watcher.Writer is NotificationCenterWebSocketWriter)
                    {
                        _websocketClients--;
                        if (_websocketClients == 0)
                        {
                            TaskExecutor.CompleteAndReplace(ref _allWebSocketsRemoved);
                        }
                    }
                }
            });
        }

        private void StartBackgroundWorkers()
        {
            foreach (var worker in BackgroundWorkers)
            {
                worker.Start();
            }
        }

        private void StopBackgroundWorkers()
        {
            foreach (var worker in BackgroundWorkers)
            {
                worker.Stop();
            }
        }

        public void Dispose()
        {
            foreach (var worker in BackgroundWorkers)
            {
                worker.Dispose();
            }
        }
    }
}
