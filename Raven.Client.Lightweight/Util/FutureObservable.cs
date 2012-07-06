using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Util
{
	internal class FutureObservable<T> : IObservable<T>
	{
		private List<IObservable<T>> inners = new List<IObservable<T>>();
		private List<IObserver<T>> observers = new List<IObserver<T>>();

		public void ForceError(Exception e)
		{
			foreach (var observer in observers)
			{
				observer.OnError(e);
			}
		}

		public void Add(IObservable<T> inner)
		{
			foreach (var observer in observers)
			{
				inner.Subscribe(observer);
			}
			inners.Add(inner);
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			var disposables = inners.Select(inner => inner.Subscribe(observer)).ToList();
			observers.Add(observer);

			return new DisposableAction(() =>
			{
				observers.Remove(observer);
				foreach (var disposable in disposables)
				{
					disposable.Dispose();
				}
			});
		}
	}
}