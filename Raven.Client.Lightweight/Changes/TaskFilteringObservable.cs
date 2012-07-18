using System;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Changes
{
	public class TaskFilteringObservable<T> : IObservableWithTask<T>
	{
		private readonly Func<T, bool> filter;
		private readonly Action onSubscriptionDisposal;

		public TaskFilteringObservable(
			Task<IObservable<T>> task, 
			Func<T, bool> filter, 
			Action onSubscriptionDisposal)
		{
			this.filter = filter;
			this.onSubscriptionDisposal = onSubscriptionDisposal;
			this.task = task;
		}

		private readonly Task<IObservable<T>> task;

		public Task  Task
		{
			get { return task; }
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			var filterer = new ErrorHidingFilteringObserver(observer, filter);
			var disposable = task.Result.Subscribe(filterer);
			return new DisposableAction(() =>
			{
				onSubscriptionDisposal();
				disposable.Dispose();
			});
		}

		private class ErrorHidingFilteringObserver : IObserver<T>
		{
			private readonly IObserver<T> inner;
			private readonly Func<T, bool> filter;

			public ErrorHidingFilteringObserver(IObserver<T> inner, Func<T, bool> filter)
			{
				this.inner = inner;
				this.filter = filter;
			}

			public void OnNext(T value)
			{
				if (filter(value) == false) 
					return;
				inner.OnNext(value);
			}

			public void OnError(Exception error)
			{
				OnCompleted();
			}

			public void OnCompleted()
			{
				inner.OnCompleted();
			}
		}
	}
}