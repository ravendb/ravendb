using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Database.Indexing
{
	public interface IBackgroundTaskExecuter
	{
		double MaxNumberOfParallelProcessingTasksRatio { get; }

		void ExecuteAllBuffered<T>(WorkContext context, IList<T> source, Action<IEnumerator<T>> action);

		void ExecuteAllInterleaved<T>(WorkContext context, IList<T> result, Action<T> action);
	}

}