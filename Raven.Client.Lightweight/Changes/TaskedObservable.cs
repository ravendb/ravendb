using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Util;

namespace Raven.Client.Changes
{
	public class TaskedObservable<T> : IObservableWithTask<T>
	{
		private readonly LocalConnectionState localConnectionState;
		private readonly Func<T, bool> filter;
		private readonly ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();

		internal TaskedObservable(
			LocalConnectionState localConnectionState, 
			Func<T, bool> filter)
		{
			this.localConnectionState = localConnectionState;
			this.filter = filter;
			Task = localConnectionState.Task;
		}

		public Task Task { get; private set; }

		public IDisposable Subscribe(IObserver<T> observer)
		{
			subscribers.TryAdd(observer);
			return new DisposableAction(() =>
			{
				localConnectionState.Dec();
				subscribers.TryRemove(observer);
			});
		}

		public void Send(T msg)
		{
			if (filter(msg) == false)
				return;

			foreach (var subscriber in subscribers)
			{
				subscriber.OnNext(msg);
			}
		}

		public void Error(Exception obj)
		{
			foreach (var subscriber in subscribers)
			{
				subscriber.OnError(obj);
			}
		}
	}
}