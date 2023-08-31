namespace Raven.Server.Dashboard
{
    public delegate bool CanAccessDatabase(string databaseName, bool requiresWrite);

    public abstract class AbstractDashboardNotification
    {
        // marker interface
    }
}
