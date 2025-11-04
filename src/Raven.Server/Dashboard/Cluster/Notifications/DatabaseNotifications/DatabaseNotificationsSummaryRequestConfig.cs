namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class DatabaseNotificationsSummaryRequestConfig
{
    public NotificationTypeConfig Alerts { get; set; }
    public NotificationTypeConfig PerformanceHints { get; set; }
}
