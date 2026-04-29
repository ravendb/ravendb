using System.Threading;

namespace Sparrow.Utils
{
    internal static class AsyncContextHelper
    {
        /// <summary>
        /// When set to true, async continuations will be posted back to the captured
        /// SynchronizationContext (if any). Used by the backup thread to keep all work
        /// on the dedicated backup thread via ExclusiveSynchronizationContext pump.
        /// Default is false, preserving ConfigureAwait(false) behavior for the client.
        /// </summary>
        internal static readonly AsyncLocal<bool> ContinueOnCapturedContext = new();
    }
}
