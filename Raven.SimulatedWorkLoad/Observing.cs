using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using Raven.Database.Util;

namespace Raven.SimulatedWorkLoad
{
	public class Observing<T> : IObservable<T>
	{
		private readonly IEnumerator<T> enumerator;
		private readonly ConcurrentSet<IObserver<T>> observers = new ConcurrentSet<IObserver<T>>();

		public bool Completed { get; private set; }

		public Observing(IEnumerable<T> src)
		{
			enumerator = src.GetEnumerator();
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			observers.Add(observer);
			return Disposable.Create(() => observers.TryRemove(observer));
		}

		public void Release(int count)
		{
			if (Completed)
				return;

			for (int i = 0; i < count; i++)
			{
				if (enumerator.MoveNext() == false)
				{
					foreach (var observer in observers)
					{
						observer.OnCompleted();
					}
					Completed = true;
					break;
				}
				foreach (var observer in observers)
				{
					observer.OnNext(enumerator.Current);
				}
			}
		}
	}
}