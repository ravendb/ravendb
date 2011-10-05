#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="IShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Client.Connection;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session
	/// </summary>
	public interface IShardAccessStrategy
	{
		/// <summary>
		/// Applies the specified action to all shard sessions.
		/// </summary>
		IList<T> Apply<T>(
			IList<IDatabaseCommands> commands,
			Func<IDatabaseCommands, int, T> operation
			) ;
	}
}
#endif
