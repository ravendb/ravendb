﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.Dashboard;

public sealed class ThreadsInfoNotificationSender : BackgroundWorkBase
{
    private readonly ConcurrentSet<ConnectedWatcher> _watchers;
    private readonly TimeSpan _notificationsThrottle;
    private readonly ThreadsUsage _threadsUsage;
    private DateTime _lastSentNotification = DateTime.MinValue;

    public ThreadsInfoNotificationSender(string resourceName,
        ConcurrentSet<ConnectedWatcher> watchers, TimeSpan notificationsThrottle, CancellationToken shutdown)
        : base(resourceName, RavenLogManager.Instance.GetLoggerForServer<ThreadsInfoNotificationSender>(), shutdown)
    {
        _watchers = watchers;
        _notificationsThrottle = notificationsThrottle;
        _threadsUsage = new ThreadsUsage();
    }

    protected override async Task DoWork()
    {
        var now = DateTime.UtcNow;
        var timeSpan = now - _lastSentNotification;
        if (timeSpan < _notificationsThrottle)
        {
            await WaitOrThrowOperationCanceled(_notificationsThrottle - timeSpan);
        }

        try
        {
            if (CancellationToken.IsCancellationRequested)
                return;

            if (_watchers.IsEmpty)
                return;

            var threadsInfo = _threadsUsage.Calculate();
            foreach (var watcher in _watchers)
            {
                // serialize to avoid race conditions
                // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                watcher.Enqueue(threadsInfo.ToJson());
            }
        }
        finally
        {
            _lastSentNotification = DateTime.UtcNow;
        }
    }
}
