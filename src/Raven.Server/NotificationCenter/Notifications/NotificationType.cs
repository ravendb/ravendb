namespace Raven.Server.NotificationCenter.Notifications
{
    public enum NotificationType
    {
        None,
        AlertRaised,
        OperationChanged,
        ResourceChanged,
        NotificationUpdated,
        RecentError, // used in studio
        PerformanceHint,
    }
}