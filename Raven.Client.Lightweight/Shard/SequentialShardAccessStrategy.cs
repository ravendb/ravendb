//-----------------------------------------------------------------------
// <copyright file="SequentialShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
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
				}
			}

			return list.ToArray();
		}

		public event ShardingErrorHandle OnError;
	}
}
#endif