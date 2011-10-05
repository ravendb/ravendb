//-----------------------------------------------------------------------
// <copyright file="ParallelShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5 && !SILVERLIGHT

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Document;

namespace Raven.Client.Shard.ShardStrategy.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session in parallel
	/// </summary>
	public class ParallelShardAccessStrategy: IShardAccessStrategy
	{
		/// <summary>
		/// Applies the specified action to all shard sessions in parallel
		/// </summary>
		public IList<T> Apply<T>(IList<IDatabaseCommands> commands, Func<IDatabaseCommands, T> operation) where T : class
		{
			var returnedLists = new T[commands.Count];

			commands
				.Select((cmd,i) =>
					Task.Factory
						.StartNew(() => operation(cmd))
						.ContinueWith(task =>
						{
							returnedLists[i] = task.Result;
						})
				)
				.WaitAll();

			return returnedLists
				.Where(x => x != null)
				.ToArray();
		}
	}
}
#endif