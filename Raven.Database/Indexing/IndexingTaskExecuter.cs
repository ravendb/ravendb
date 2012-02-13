using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;

namespace Raven.Database.Indexing
{
	public static class IndexingTaskExecuter
	{
		public static void ExecuteAll<T>(InMemoryRavenConfiguration configuration, TaskScheduler scheduler, IEnumerable<T> source, Action<T, long> action)
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

			foreach (var partitioned in Partition(source, configuration.MaxNumberOfParallelIndexTasks))
			{
				Parallel.ForEach(partitioned, new ParallelOptions
				{
					TaskScheduler = scheduler,
					MaxDegreeOfParallelism = configuration.MaxNumberOfParallelIndexTasks
				},(item,_,index)=>action(item, index));
			}
		}

		static IEnumerable<IEnumerable<T>> Partition<T>(IEnumerable<T> source, int size)
		{
			var shouldContinue = new Reference<bool>
			{
				Value = true
			};
			var enumerator = source.GetEnumerator();
			while (shouldContinue.Value)
			{
				yield return ParitionInternal(enumerator, size, shouldContinue);
			}
		}

		private static IEnumerable<T> ParitionInternal<T>(IEnumerator<T> enumerator, int size, Reference<bool> shouldContinue)
		{
			for (int i = 0; i < size; i++)
			{
				shouldContinue.Value = enumerator.MoveNext();
				if (shouldContinue.Value == false)
					break;
				yield return enumerator.Current;
			}
		}
	}
}