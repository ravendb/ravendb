using System.Collections.Generic;

namespace Raven.Server.Dashboard.DatabaseNotifications;

public sealed class DatabaseNotificationsSummary : AbstractDashboardNotification
{
    public List<DatabaseNotificationsSummaryItem> Items { get; set; }

    public DatabaseNotificationsSummary()
    {
        Items = new List<DatabaseNotificationsSummaryItem>();
    }
}
