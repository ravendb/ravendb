namespace Raven.Client.Indexing
{
    public enum IndexLockMode
    {
        Unlock,
        LockedIgnore,
        LockedError,
        SideBySide
    }
}
