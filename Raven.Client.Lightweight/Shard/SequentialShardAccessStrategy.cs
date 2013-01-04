//-----------------------------------------------------------------------
// <copyright file="SequentialShardAccessStrategy.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !SILVERLIGHT
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;

namespace Raven.Client.Shard
{
	/// <summary>
	/// Apply an operation to all the shard session in sequence
	/// </summary>
	public class SequentialShardAccessStrategy : IShardAccessStrategy
	{
		public event ShardingErrorHandle<IDatabaseCommands> OnError;

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
					if (error(commands[i], request, e) == false)
					{
						throw;
					}
					errors.Add(e);
				}
			}

			// if ALL nodes failed, we still throw
			if (errors.Count == commands.Count)
				throw new AggregateException(errors);

			return list.ToArray();
		}

		public event ShardingErrorHandle<IAsyncDatabaseCommands> OnAsyncError;

		public Task<T[]> ApplyAsync<T>(IList<IAsyncDatabaseCommands> commands, ShardRequestData request, Func<IAsyncDatabaseCommands, int, Task<T>> operation)
		{
			var resultsTask = new TaskCompletionSource<List<T>>();
			var results = new List<T>();
			var errors = new List<Exception>();

			Action<int> executer = null;
			executer = index =>
				{
					if (index >= commands.Count)
					{
						if (errors.Count == commands.Count)
							throw new AggregateException(errors);
						// finished all commands successfully
						resultsTask.SetResult(results);
						return;
					}

					operation(commands[index], index).ContinueWith(task =>
					{
						if (task.IsFaulted)
						{
							var error = OnAsyncError;
							if (error == null)
							{
								resultsTask.SetException(task.Exception);
								return;
							}
							if (error(commands[index], request, task.Exception) == false)
							{
								resultsTask.SetException(task.Exception);
								return;
							}
							errors.Add(task.Exception);
						}
						else
						{
							results.Add(task.Result);
						}

						// After we've dealt with one result, we call the operation on the next shard
						executer(index + 1);
					});
				};

			executer(0);
			return resultsTask.Task.ContinueWith(task => task.Result.ToArray());
		}
	}
}
#endif