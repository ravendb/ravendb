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
		private readonly Action onSubscriptionDisposal;
		private ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();

		public TaskedObservable(
			Task task, 
			Func<T, bool> filter, 
			Action onSubscriptionDisposal)
		{
			this.onSubscriptionDisposal = onSubscriptionDisposal;
			Task = task;
		}

		public Task Task { get; private set; }

		public IDisposable Subscribe(IObserver<T> observer)
		{
			subscribers.TryAdd(observer);
			return new DisposableAction(() =>
			{
				onSubscriptionDisposal();
				subscribers.TryRemove(observer);
			});
		}

		public void Send(T msg)
		{
			foreach (var subscriber in subscribers)
			{
				subscriber.OnNext(msg);
			}
		}
	}
}