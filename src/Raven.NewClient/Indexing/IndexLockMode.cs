namespace Raven.NewClient.Abstractions.Indexing
{
    public enum IndexLockMode
    {
        Unlock,
        LockedIgnore,
        LockedError,
        SideBySide
    }
}
