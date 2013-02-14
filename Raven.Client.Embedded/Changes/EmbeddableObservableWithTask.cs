using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Database.Util;

namespace Raven.Client.Embedded.Changes
{
	internal class EmbeddableObservableWithTask<T> : IObservableWithTask<T>
	{
		readonly ConcurrentSet<IObserver<T>> registered = new ConcurrentSet<IObserver<T>>();

		public IDisposable Subscribe(IObserver<T> observer)
		{
			registered.Add(observer);
			return new DisposableAction(() => registered.TryRemove(observer));
		}

		public Task<IObservable<T>>  Task { get; private set; }

		public EmbeddableObservableWithTask()
		{
			Task = new CompletedTask<IObservable<T>>(this);
		}

		public void Notify(object sender, T e)
		{
			foreach (var observer in registered)
			{
				observer.OnNext(e);
			}
		}


	}
}