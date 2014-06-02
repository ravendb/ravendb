using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem.Shard
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
        event ShardingErrorHandle<AsyncFilesServerClient> OnAsyncError;

        /// <summary>
        /// Applies the specified action to all shard sessions.
        /// </summary>
        Task<T[]> ApplyAsync<T>(IList<AsyncFilesServerClient> commands, ShardRequestData request, Func<AsyncFilesServerClient, int, Task<T>> operation);
    }

    public delegate bool ShardingErrorHandle<TRavenFileSystemClient>(TRavenFileSystemClient failingCommands, ShardRequestData request, Exception exception);
}
