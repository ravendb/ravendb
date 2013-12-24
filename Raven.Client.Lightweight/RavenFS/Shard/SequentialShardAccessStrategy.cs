using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS.Shard
{
	/// <summary>
	/// Apply an operation to all the shard session in sequence
	/// </summary>
	public class SequentialShardAccessStrategy : IShardAccessStrategy
	{
		public event ShardingErrorHandle<RavenFileSystemClient> OnAsyncError;

		public Task<T[]> ApplyAsync<T>(IList<RavenFileSystemClient> commands, ShardRequestData request, Func<RavenFileSystemClient, int, Task<T>> operation)
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
