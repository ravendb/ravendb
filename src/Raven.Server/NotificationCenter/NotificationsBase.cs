using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Raven.Server.Background;
using Sparrow.Collections;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter
{
    public abstract class NotificationsBase : IDisposable
    {
        private readonly object _watchersLock = new object();

        protected ConcurrentSet<ConnectedWatcher> Watchers { get; }
        protected List<BackgroundWorkBase> BackgroundWorkers { get; }

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
                    StartBackgroundWorkers();
            }

            return new DisposableAction(() =>
            {
                lock (_watchersLock)
                {
                    Watchers.TryRemove(watcher);

                    if (Watchers.Count == 0)
                        StopBackgroundWorkers();
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
