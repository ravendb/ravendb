namespace Raven.Client.Documents.Replication
{
    public enum StraightforwardConflictResolution
    {
        None,
        /// <summary>
        /// Always resolve in favor of the latest version based on the last modified time
        /// </summary>
        ResolveToLatest
    }
}