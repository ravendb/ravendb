namespace Raven.Client.Documents.Session
{
    public enum ConcurrencyCheckMode
    {
        /// <summary>
        /// Automatic optimistic concurrency check depending on <see cref="OptimisticConcurrencyMode"/> setting or provided Change Vector
        /// </summary>
        Auto,

        /// <summary>
        /// Force optimistic concurrency check even if <see cref="OptimisticConcurrencyMode"/> is <see cref="OptimisticConcurrencyMode.None"/>
        /// </summary>
        Forced,

        /// <summary>
        /// Disable optimistic concurrency check for this entity's PUT/DELETE command even if <see cref="OptimisticConcurrencyMode"/> is set.<br/>
        /// Note: when <see cref="OptimisticConcurrencyMode.WritesAndReads"/> is active, the entity is still verified
        /// during <see cref="DocumentSession.SaveChanges"/> to ensure it was not modified by another session.
        /// </summary>
        Disabled
    }
}