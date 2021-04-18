namespace Raven.Client.Documents.Indexes
{
    public enum IndexRunningStatus
    {
        Running,
        Paused,
        Disabled,

        // for rolling indexes
        Pending
    }
}
