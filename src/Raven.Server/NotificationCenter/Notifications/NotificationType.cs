namespace Raven.Server.NotificationCenter.Notifications
{
    public enum NotificationType
    {
        None,
        AlertRaised,
        OperationChanged,
        DatabaseChanged,
        NotificationUpdated,
        RecentError, // used in studio
        PerformanceHint,
        DatabaseStatsChanged,
        ClusterTopologyChanged
    }
}