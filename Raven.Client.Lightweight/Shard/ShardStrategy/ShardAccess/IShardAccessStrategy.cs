#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="IShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
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
		/// <typeparam name="T"></typeparam>
		/// <param name="shardSessions">The shard sessions.</param>
		/// <param name="operation">The operation.</param>
		/// <returns></returns>
		IList<T> Apply<T>(
			IList<IDocumentSession> shardSessions,
			Func<IDocumentSession, IList<T>> operation
			);
	}
}
#endif
