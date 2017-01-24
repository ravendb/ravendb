namespace Raven.Server.NotificationCenter.Actions
{
    public enum ActionType
    {
        None,
        AlertRaised,
        OperationChanged,
        ResourceChanged,
        NotificationUpdated
        // performance, hints?
    }
}