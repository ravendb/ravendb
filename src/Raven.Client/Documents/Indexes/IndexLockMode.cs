namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Defines the lock modes that control the behavior of index modifications.
    /// </summary>
    /// <value>
    /// <list type="bullet">
    /// <item>
    /// <term>Unlock</term>
    /// <description>Allows all index modifications.</description>
    /// </item>
    /// <item>
    /// <term>LockedIgnore</term>
    /// <description>Ignores all modification attempts without raising errors.</description>
    /// </item>
    /// <item>
    /// <term>LockedError</term>
    /// <description>Blocks all modifications and raises errors if modification is attempted.</description>
    /// </item>
    /// </list>
    /// </value>
    public enum IndexLockMode
    {
        Unlock,
        LockedIgnore,
        LockedError
    }
}
