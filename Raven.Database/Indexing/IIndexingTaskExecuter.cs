using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Database.Config;

namespace Raven.Database.Indexing
{
	public interface IIndexingTaskExecuter
	{
		void ExecuteAll<T>(InMemoryRavenConfiguration configuration, TaskScheduler scheduler, IList<T> source, Action<T, long> action);
	}
}