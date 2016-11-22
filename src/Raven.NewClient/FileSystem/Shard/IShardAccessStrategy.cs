using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem.Shard
{
    /// <summary>
    /// Apply an operation to all the shard session
    /// </summary>
    public interface IShardAccessStrategy
    {
        /// <summary>
        /// Occurs on error, allows to handle an error one (or more) of the nodes
        /// is failing
        /// </summary>
        event ShardingErrorHandle<IAsyncFilesCommands> OnAsyncError;

        /// <summary>
        /// Applies the specified action to all shard sessions.
        /// </summary>
        Task<T[]> ApplyAsync<T>(IList<IAsyncFilesCommands> commands, ShardRequestData request, Func<IAsyncFilesCommands, int, Task<T>> operation);
    }

    public delegate bool ShardingErrorHandle<TRavenFileSystemClient>(TRavenFileSystemClient failingCommands, ShardRequestData request, Exception exception);
}
