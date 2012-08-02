using System;
using System.Threading.Tasks;
using Raven.Client.Changes;

namespace Raven.Client.Embedded.Changes
{
	internal class FilteringObservableWithTask<T> : IObservableWithTask<T>
	{
		private readonly IObservableWithTask<T> inner;
		private Func<T, bool> filter;

		public FilteringObservableWithTask(IObservableWithTask<T> inner, Func<T, bool> filter)
		{
			this.inner = inner;
			this.filter = filter;
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			return inner.Subscribe(new ErrorHidingFilteringObserver(observer, filter));
		}

		public Task Task
		{
			get { return inner.Task; }
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