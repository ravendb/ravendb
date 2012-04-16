#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="IShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client.Connection;

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
		event ShardingErrorHandle OnError;

		/// <summary>
		/// Applies the specified action to all shard sessions.
		/// </summary>
		T[] Apply<T>(IList<IDatabaseCommands> commands, ShardRequestData request, Func<IDatabaseCommands, int, T> operation);
	}

	public delegate bool ShardingErrorHandle(IDatabaseCommands failingCommands, ShardRequestData request, Exception exception);
}
#endif