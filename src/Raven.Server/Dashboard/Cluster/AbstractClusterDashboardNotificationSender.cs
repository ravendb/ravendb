// -----------------------------------------------------------------------
//  <copyright file="Widget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster
{
    public abstract class AbstractClusterDashboardNotificationSender : BackgroundWorkBase
    {
        private readonly int _widgetId;
        private readonly ConnectedWatcher _watcher;
        
        private DateTime _lastSentNotification = DateTime.MinValue;

        protected AbstractClusterDashboardNotificationSender(int widgetId, ConnectedWatcher watcher, CancellationToken shutdown) : base(nameof(AbstractClusterDashboardNotificationSender), shutdown)
        {
            _widgetId = widgetId;
            _watcher = watcher;
        }

        protected abstract TimeSpan NotificationInterval { get; }

        protected abstract AbstractClusterDashboardNotification CreateNotification();

        protected override async Task DoWork()
        {
            var now = SystemTime.UtcNow;
            var timeSpan = now - _lastSentNotification;
            if (timeSpan < NotificationInterval)
            {
                await WaitOrThrowOperationCanceled(NotificationInterval - timeSpan);
            }

            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                var notification = CreateNotification();

                if (_watcher.Filter != null)
                {
                    var filteredJson = notification.ToJsonWithFilter(_watcher.Filter);

                    if (filteredJson != null)
                    {
                        _watcher.NotificationsQueue.Enqueue(new DynamicJsonValue
                        {
                            [nameof(WidgetMessage.Id)] = _widgetId,
                            [nameof(WidgetMessage.Data)] = filteredJson
                        });
                    }
                }
                else
                {
                    // serialize to avoid race conditions
                    // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                    _watcher.NotificationsQueue.Enqueue(new DynamicJsonValue
                    {
                        [nameof(WidgetMessage.Id)] = _widgetId,
                        [nameof(WidgetMessage.Data)] = notification.ToJson()
                    });
                }
            }
            finally
            {
                _lastSentNotification = SystemTime.UtcNow;
            }
        }
    }
}
