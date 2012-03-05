#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="IShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client.Connection;

namespace Raven.Client.Shard.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session
	/// </summary>
	public interface IShardAccessStrategy
	{
		/// <summary>
		/// Applies the specified action to all shard sessions.
		/// </summary>
		T[] Apply<T>(IList<IDatabaseCommands> commands, Func<IDatabaseCommands, int, T> operation);
	}
}
#endif