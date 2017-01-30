namespace Raven.Server.NotificationCenter.Notifications
{
    public enum NotificationType
    {
        None,
        AlertRaised,
        OperationChanged,
        ResourceChanged,
        NotificationUpdated
        // performance, hints?
    }
}