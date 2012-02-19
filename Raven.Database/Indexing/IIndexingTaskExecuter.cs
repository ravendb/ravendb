using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Database.Config;

namespace Raven.Database.Indexing
{
	public interface IIndexingTaskExecuter
	{
		IList<TResult> Apply<T, TResult>(IEnumerable<T> source, Func<T, TResult> func)
			where TResult : class;
		void ExecuteAll<T>(InMemoryRavenConfiguration configuration, TaskScheduler scheduler, IList<T> source, Action<T, long> action);
	}
}