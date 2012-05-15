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
using Raven.Client.Extensions;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Apply an operation to all the shard session in parallel
	/// </summary>
	public class ParallelShardAccessStrategy : IShardAccessStrategy
	{
		/// <summary>
		/// Applies the specified action to all shard sessions in parallel
		/// </summary>
		public T[] Apply<T>(IList<IDatabaseCommands> commands, ShardRequestData request, Func<IDatabaseCommands, int, T> operation)
		{
			var returnedLists = new T[commands.Count];
			var valueSet = new bool[commands.Count];
			var errors = new Exception[commands.Count];
			commands
				.Select((cmd, i) =>
				        Task.Factory.StartNew(() => operation(cmd, i))
				        	.ContinueWith(task =>
				        	{
				        		try
				        		{
				        			returnedLists[i] = task.Result;
				        			valueSet[i] = true;
				        		}
				        		catch (Exception e)
				        		{
				        			var error = OnError;
				        			if (error == null)
				        				throw;
				        			if (error(commands[i], request, e) == false)
				        			{
				        				throw;
				        			}
				        			errors[i] = e;
				        		}
				        	})
				)
				.WaitAll();

			// if ALL nodes failed, we still throw
			if (errors.All(x => x != null))
				throw new AggregateException(errors);

			return returnedLists.Where((t, i) => valueSet[i]).ToArray();
		}

		public event ShardingErrorHandle OnError;
	}
}
#endif