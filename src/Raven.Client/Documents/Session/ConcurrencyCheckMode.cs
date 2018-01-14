namespace Raven.Client.Documents.Session
{
    public enum ConcurrencyCheckMode
    {
        /// <summary>
        /// Automatic optimistic concurrency check depending on UseOptimisticConcurrency setting or provided Change Vector
        /// </summary>
        Auto,

        /// <summary>
        /// Force optimistic concurrency check even if UseOptimisticConcurrency is not set
        /// </summary>
        Forced,

        /// <summary>
        /// Disable optimistic concurrency check even if UseOptimisticConcurrency is set
        /// </summary>
        Disabled
    }
}