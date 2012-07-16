using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Util
{
	public class TaskObservable<T> : IObservable<T>
	{
		public TaskObservable(Task<IObservable<T>> task)
		{
			Task = task;
		}

		public Task<IObservable<T>> Task { get; private set; }
		
		public IDisposable Subscribe(IObserver<T> observer)
		{
			var disposeTask = Task.ContinueWith(task => task.Result.Subscribe(new ErrorHidingObserver(observer)));
			
			return new DisposableAction(() =>
			{
				disposeTask.ContinueWith(task => task.Result.Dispose());
			});
		}

		private class ErrorHidingObserver : IObserver<T>
		{
			private readonly IObserver<T> inner;

			public ErrorHidingObserver(IObserver<T> inner)
			{
				this.inner = inner;
			}

			public void OnNext(T value)
			{
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