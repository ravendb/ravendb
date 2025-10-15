using System.Collections.Generic;

namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class NotificationTypeConfig
{
    public bool IsEnabled { get; set; }
    public HashSet<string> Reasons { get; set; }
}
