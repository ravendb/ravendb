//-----------------------------------------------------------------------
// <copyright file="IShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;

namespace Raven.Client.Shard
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
		event ShardingErrorHandle<IDatabaseCommands> OnError;

		/// <summary>
		/// Applies the specified action to all shard sessions.
		/// </summary>
		T[] Apply<T>(IList<IDatabaseCommands> commands, ShardRequestData request, Func<IDatabaseCommands, int, T> operation);

		/// <summary>
		/// Occurs on error, allows to handle an error one (or more) of the nodes
		/// is failing
		/// </summary>
		event ShardingErrorHandle<IAsyncDatabaseCommands> OnAsyncError;

		/// <summary>
		/// Applies the specified action to all shard sessions.
		/// </summary>
		Task<T[]> ApplyAsync<T>(IList<IAsyncDatabaseCommands> commands, ShardRequestData request, Func<IAsyncDatabaseCommands, int, Task<T>> operation);
	}

	public delegate bool ShardingErrorHandle<TDatabaseCommands>(TDatabaseCommands failingCommands, ShardRequestData request, Exception exception);
}
