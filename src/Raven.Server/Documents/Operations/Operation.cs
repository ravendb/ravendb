using System;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Documents.Operations;

public class Operation : AbstractOperation
{
    private readonly TimeSpan _throttleTime = TimeSpan.FromSeconds(1);

    private readonly ThrottledNotification _throttle = new();

    public void NotifyCenter(OperationChanged notification, Action<OperationChanged> addToNotificationCenter)
    {
        if (ShouldThrottleMessage(notification) == false)
        {
            addToNotificationCenter(notification);
            return;
        }

        // let us throttle changes about the operation progress

        var now = SystemTime.UtcNow;

        _throttle.Notification = notification;

        var sinceLastSent = now - _throttle.SentAt;

        if (_throttle.Scheduled == null && sinceLastSent > _throttleTime)
        {
            addToNotificationCenter(_throttle.Notification);
            _throttle.SentAt = now;

            return;
        }

        if (_throttle.Scheduled == null)
        {
            _throttle.Scheduled = System.Threading.Tasks.Task.Delay(_throttleTime - sinceLastSent).ContinueWith(x =>
            {
                if (State.Status == OperationStatus.InProgress)
                    addToNotificationCenter(_throttle.Notification);

                _throttle.SentAt = DateTime.UtcNow;
                _throttle.Scheduled = null;
            });
        }

        static bool ShouldThrottleMessage(OperationChanged notification)
        {
            if (notification.State.Status != OperationStatus.InProgress)
            {
                return false;
            }

            return true;
        }
    }
}
