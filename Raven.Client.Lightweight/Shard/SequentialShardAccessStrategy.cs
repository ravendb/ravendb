//-----------------------------------------------------------------------
// <copyright file="SequentialShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Connection;

namespace Raven.Client.Shard
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
		public T[] Apply<T>(IList<IDatabaseCommands> commands, ShardRequestData request, Func<IDatabaseCommands, int, T> operation)
		{
			var list = new List<T>();
			var errors = new List<Exception>();
			for (int i = 0; i < commands.Count; i++)
			{
				try
				{
					list.Add(operation(commands[i], i));
				}
				catch (Exception e)
				{
					var error = OnError;
					if (error == null)
						throw;
					if(error(commands[i], request, e) == false)
					{
						throw;
					}
					errors.Add(e);
				}
			}

			// if ALL nodes failed, we still throw
			if (errors.Count == commands.Count)
#if !NET_3_5
				throw new AggregateException(errors);
#else
			throw new InvalidOperationException("Got an error from all servers", errors.First())
				{
					Data = {{"Errors", errors}}
				};
#endif

			return list.ToArray();
		}

		public event ShardingErrorHandle OnError;
	}
}
#endif