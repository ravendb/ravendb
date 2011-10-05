#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="SequentialShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Connection;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session in sequence
	/// </summary>
	public class SequentialShardAccessStrategy : IShardAccessStrategy
	{
		/// <summary>
		/// Applies the specified action for all shard sessions.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="commands">The shard sessions.</param>
		/// <param name="operation">The operation.</param>
		/// <returns></returns>
		public IList<T> Apply<T>(IList<IDatabaseCommands> commands, Func<IDatabaseCommands, T> operation) where T : class
		{
			return commands.Select(operation).ToList();
		}
	}
}
#endif
