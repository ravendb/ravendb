using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Extensions;

namespace Raven.Client.Shard
{
	public class ShardedObservableWithTask<T> : IObservableWithTask<T>
	{
		private readonly IObservableWithTask<T>[] inner;

		public ShardedObservableWithTask(IObservableWithTask<T>[] inner)
		{
			this.inner = inner;
			Task = System.Threading.Tasks.Task.Factory.ContinueWhenAll(inner.Select(x => x.Task).ToArray(), tasks =>
			{
				foreach (var task in tasks)
				{
					task.AssertNotFailed();
				}
				return (IObservable<T>)this;
			});
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			var disposables = inner.Select(x=>x.Subscribe(observer)).ToArray();
			return new DisposableAction(() =>
			{
				foreach (var disposable in disposables)
				{
					disposable.Dispose();
				}
			});
		}

		public Task<IObservable<T>>  Task { get; private set; }
	}
}