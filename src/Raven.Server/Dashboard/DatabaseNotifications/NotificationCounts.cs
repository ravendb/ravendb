using System.Collections.Generic;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Dashboard.DatabaseNotifications;

public class NotificationCounts
{
    public Dictionary<NotificationType, ReasonCounts> ByType { get; } = new();

    public void Increment(NotificationType notificationType, string reason)
    {
        if (reason == null)
            return;
        
        if (ByType.TryGetValue(notificationType, out var reasonCounts) == false)
        {
            reasonCounts = new ReasonCounts();
            ByType[notificationType] = reasonCounts;
        }

        reasonCounts.Increment(reason);
    }
}
