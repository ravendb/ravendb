using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class DatabaseNotificationsSummary
{
    public string DatabaseName { get; set; }
    public List<NotificationSummaryItem> PerformanceHints { get; set; } = [];
    public List<NotificationSummaryItem> Alerts { get; set; } = [];
    public long AlertsCount { get; set; }
    public long PerformanceHintsCount { get; set; }
    
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DatabaseName)] = DatabaseName,
            [nameof(PerformanceHints)] = new DynamicJsonArray(PerformanceHints.Select(x => x.ToJson())),
            [nameof(Alerts)] = new DynamicJsonArray(Alerts.Select(x => x.ToJson())),
            [nameof(AlertsCount)] = AlertsCount,
            [nameof(PerformanceHintsCount)] = PerformanceHintsCount
        };
    }
}
