#if !NET_3_5

using Raven.Client.Client.Async;

namespace Raven.Client
{
    /// <summary>
    /// Advanced async session operations
    /// </summary>
    public interface IAsyncAdvancedSessionOperations : IAdvancedDocumentSessionOperations
    {
        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        IAsyncDatabaseCommands AsyncDatabaseCommands { get; }
    }
}
#endif