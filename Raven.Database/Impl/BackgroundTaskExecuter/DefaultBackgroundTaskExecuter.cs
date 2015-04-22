using Raven.Abstractions.Logging;
using Raven.Database.Config;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Indexing
{
	public class DefaultBackgroundTaskExecuter : IBackgroundTaskExecuter, ICpuUsageHandler
	{
		private static readonly ILog logger = LogManager.GetCurrentClassLogger();

		private double maxNumberOfParallelProcessingTasksRatio = 1;

		public DefaultBackgroundTaskExecuter()
		{
			CpuStatistics.RegisterCpuUsageHandler(this);
		}

		public IList<TResult> Apply<T, TResult>(WorkContext context, IEnumerable<T> source, Func<T, TResult> func)
			where TResult : class
		{
			var maxNumberOfParallelIndexTasks = context.CurrentNumberOfParallelTasks;
			if (maxNumberOfParallelIndexTasks == 1)
			{
				return source.Select(func).ToList();
			}
			var list = source.AsParallel()
				.Select(func)
				.ToList();
			for (int i = 0; i < list.Count; i++)
			{
				if (list[i] != null)
					continue;
				list.RemoveAt(i);
				i--;
			}
			return list;
		}



		/// <summary>
		/// Note that here we assume that  source may be very large (number of documents)
		/// </summary>
		public void ExecuteAllBuffered<T>(WorkContext context, IList<T> source, Action<IEnumerator<T>> action)
		{
			var maxNumberOfParallelIndexTasks = context.CurrentNumberOfParallelTasks;
			var size = Math.Max(source.Count / maxNumberOfParallelIndexTasks, 1024);
			if (maxNumberOfParallelIndexTasks == 1 || source.Count <= size)
			{
				using (var e = source.GetEnumerator())
					action(e);
				return;
			}
			int remaining = source.Count;
			int iteration = 0;
			var parts = new List<IEnumerator<T>>();
			while (remaining > 0)
			{
				parts.Add(Yield(source, iteration * size, size));
				iteration++;
				remaining -= size;
			}

			ExecuteAllInterleaved(context, parts, action);
		}

		private IEnumerator<T> Yield<T>(IList<T> source, int start, int end)
		{
			while (start < source.Count && end > 0)
			{
				end--;
				yield return source[start];
				start++;
			}
		}

		/// <summary>
		/// Note that we assume that source is a relatively small number, expected to be 
		/// the number of indexes, not the number of documents.
		/// </summary>
		public void ExecuteAll<T>(
			WorkContext context,
			IList<T> source, Action<T, long> action)
		{
			var maxNumberOfParallelProcessingTasks = context.CurrentNumberOfParallelTasks;
			if (maxNumberOfParallelProcessingTasks == 1)
			{
				long i = 0;
				foreach (var item in source)
				{
					action(item, i++);
				}
				return;
			}

			context.CancellationToken.ThrowIfCancellationRequested();
			var partitioneds = Partition(source, maxNumberOfParallelProcessingTasks).ToList();
			int start = 0;
			foreach (var partitioned in partitioneds)
			{
				context.CancellationToken.ThrowIfCancellationRequested();
				var currentStart = start;
				Parallel.ForEach(partitioned, new ParallelOptions
				{
					TaskScheduler = context.TaskScheduler,
					MaxDegreeOfParallelism = maxNumberOfParallelProcessingTasks
				}, (item, _, index) =>
				{
					using (LogContext.WithDatabase(context.DatabaseName))
					{
						action(item, currentStart + index);
					}
				});
				start += partitioned.Count;
			}
		}

		static IEnumerable<IList<T>> Partition<T>(IList<T> source, int size)
		{
			if (size <= 0)
				throw new ArgumentException("Size cannot be 0");

			for (int i = 0; i < source.Count; i += size)
			{
				yield return source.Skip(i).Take(size).ToList();
			}
		}

		public void ExecuteAllInterleaved<T>(WorkContext context, IList<T> result, Action<T> action)
		{
			if (result.Count == 0)
				return;
			if (result.Count == 1)
			{
				action(result[0]);
				return;
			}

			using (LogContext.WithDatabase(context.DatabaseName))
			using (var semaphoreSlim = new SemaphoreSlim(context.CurrentNumberOfParallelTasks))
			{
				var tasks = new Task[result.Count];
				for (int i = 0; i < result.Count; i++)
				{
					var index = result[i];
					var indexToWorkOn = index;

					var task = new Task(() => action(indexToWorkOn));
					tasks[i] = task.ContinueWith(done =>
					{
						semaphoreSlim.Release();
						return done;
					}).Unwrap();

					semaphoreSlim.Wait();

					task.Start(context.Database.BackgroundTaskScheduler);
				}

				Task.WaitAll(tasks);
			}
		}

		public void HandleHighCpuUsage()
		{
			maxNumberOfParallelProcessingTasksRatio = Math.Min(1, maxNumberOfParallelProcessingTasksRatio / 2);
		}

		public void HandleLowCpuUsage()
		{
			maxNumberOfParallelProcessingTasksRatio = Math.Min(1, maxNumberOfParallelProcessingTasksRatio * 1.2);
		}

		public double MaxNumberOfParallelProcessingTasksRatio
		{
			get { return maxNumberOfParallelProcessingTasksRatio; }
		}
	}
}