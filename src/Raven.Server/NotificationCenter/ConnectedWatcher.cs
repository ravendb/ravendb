using Raven.Server.Dashboard;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;

namespace Raven.Server.NotificationCenter
{
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
