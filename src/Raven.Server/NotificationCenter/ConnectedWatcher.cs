﻿using Raven.Server.Dashboard;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;

namespace Raven.Server.NotificationCenter
{
    public sealed class ConnectedWatcher
    {
        public AsyncQueue<DynamicJsonValue> NotificationsQueue;

        public IWebsocketWriter Writer;

        public CanAccessDatabase Filter;
    }
}
