using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Database.Config;

namespace Raven.Database.Indexing
{
	public class DefaultIndexingTaskExecuter : IIndexingTaskExecuter
	{
		public IList<TResult> Apply<T, TResult>(IEnumerable<T> source, Func<T, TResult> func)
			where TResult : class
		{
			return source.AsParallel()
				.Select(func)
				.Where(x => x != null)
				.ToList();
		}

		/// <summary>
		/// Note that we assume that source is a relatively small number, expected to be 
		/// the number of indexes, not the number of documents.
		/// </summary>
		public void ExecuteAll<T>(InMemoryRavenConfiguration configuration, TaskScheduler scheduler, IList<T> source, Action<T, long> action)
		{
			if(configuration.MaxNumberOfParallelIndexTasks == 1)
			{
				long i = 0;
				foreach (var item in source)
				{
					action(item, i++);
				}
				return;
			}

			var partitioneds = Partition(source, configuration.MaxNumberOfParallelIndexTasks).ToList();
			int start = 0;
			foreach (var partitioned in partitioneds)
			{
				var currentStart = start;
				Parallel.ForEach(partitioned, new ParallelOptions
				{
					TaskScheduler = scheduler,
					MaxDegreeOfParallelism = configuration.MaxNumberOfParallelIndexTasks
				},(item,_,index)=>action(item, currentStart + index));
				start += partitioned.Count;
			}
		}

		static IEnumerable<IList<T>> Partition<T>(IList<T> source, int size)
		{
			for (int i = 0; i < source.Count; i+=size)
			{
				yield return source.Skip(i).Take(size).ToList();
			}
		}
	}
}