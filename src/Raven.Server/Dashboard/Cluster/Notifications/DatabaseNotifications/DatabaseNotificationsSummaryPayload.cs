using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class DatabaseNotificationsSummaryPayload : AbstractClusterDashboardNotification
{
    public List<DatabaseNotificationsSummary> NotificationsSummary { get; set; } = [];
    public long AlertsCount { get; set; }
    public long PerformanceHintsCount { get; set; }
    
    public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.DatabasesNotifications;
    
    public override DynamicJsonValue ToJson()
    {
        var json = base.ToJson();
        json[nameof(NotificationsSummary)] = new DynamicJsonArray(NotificationsSummary.Select(x => x.ToJson()));
        json[nameof(AlertsCount)] = NotificationsSummary.Sum(x => x.AlertsCount);
        json[nameof(PerformanceHintsCount)] = NotificationsSummary.Sum(x => x.PerformanceHintsCount);
        return json;
    }
    
    public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
    {
        var json = base.ToJson();
        long alertsCount = 0;
        long performanceHintsCount = 0;
        
        var items = new DynamicJsonArray();
        foreach (var databaseNotificationsSummary in NotificationsSummary)
        {
            if (filter(databaseNotificationsSummary.DatabaseName, requiresWrite: false))
            {
                items.Add(databaseNotificationsSummary.ToJson());
                alertsCount += databaseNotificationsSummary.AlertsCount;
                performanceHintsCount += databaseNotificationsSummary.PerformanceHintsCount;
            }
        }

        if (items.Count == 0)
            return null;
        
        json[nameof(NotificationsSummary)] = items;
        json[nameof(AlertsCount)] = alertsCount;
        json[nameof(PerformanceHintsCount)] = performanceHintsCount;
        return json;
    }
}
