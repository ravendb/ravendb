using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Database.Indexing
{
	public interface IBackgroundTaskExecuter
	{
		IList<TResult> Apply<T, TResult>(WorkContext context, IEnumerable<T> source, Func<T, TResult> func)
			where TResult : class;

		void Repeat(IRepeatedAction action);

		void ExecuteAll<T>(WorkContext context, IList<T> source, Action<T, long> action);

		void ExecuteAllBuffered<T>(WorkContext context, IList<T> source, Action<IEnumerator<T>> action);

		void ExecuteAllInterleaved<T>(WorkContext context, IList<T> result, Action<T> action);
	}

	public interface IRepeatedAction
	{
		TimeSpan RepeatDuration { get; }
		bool IsValid { get; }
		void Execute();
	}
}