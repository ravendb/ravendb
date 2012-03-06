//-----------------------------------------------------------------------
// <copyright file="ParallelShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NET_3_5 && !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Connection;

namespace Raven.Client.Shard.ShardAccess
{
	/// <summary>
	/// Apply an operation to all the shard session in parallel
	/// </summary>
	public class ParallelShardAccessStrategy: IShardAccessStrategy
	{
		/// <summary>
		/// Applies the specified action to all shard sessions in parallel
		/// </summary>
		public T[] Apply<T>(ICollection<IDatabaseCommands> commands, Func<IDatabaseCommands,int, T> operation)
		{
			var returnedLists = new T[commands.Count];

			commands
				.Select((cmd, i) =>
				        Task.Factory.StartNew(() => operation(cmd, i))
				        	.ContinueWith(task =>
				        	              	{
				        	              		returnedLists[i] = task.Result;
				        	              	})
				)
				.WaitAll();

			return returnedLists.ToArray();
		}
	}
}
#endif