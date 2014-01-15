using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Shard
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
		event ShardingErrorHandle<RavenFileSystemClient> OnAsyncError;

		/// <summary>
		/// Applies the specified action to all shard sessions.
		/// </summary>
		Task<T[]> ApplyAsync<T>(IList<RavenFileSystemClient> commands, ShardRequestData request, Func<RavenFileSystemClient, int, Task<T>> operation);
	}

	public delegate bool ShardingErrorHandle<TRavenFileSystemClient>(TRavenFileSystemClient failingCommands, ShardRequestData request, Exception exception);
}
