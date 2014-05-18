using System;
using Raven.Abstractions.Extensions;

namespace Raven.Client.RavenFS.Changes
{
	internal abstract class NotificationSubject
	{
		public abstract void OnNext(Notification notification);
		public abstract void OnError(Exception exception);
		public abstract void OnCompleted();
	}

	internal class NotificationSubject<T> : NotificationSubject, IObservable<T> where T : Notification
	{
		private readonly Func<T, bool> filter;
		private ImmutableList<IObserver<T>> subscribers = new ImmutableList<IObserver<T>>();
		private Action onFirstSubscriber;
		private Action onAllUnsubscribed;
		private object gate = new object();
		private Exception exception;
		private bool isComplete;

		public NotificationSubject(Action onFirstSubscriber, Action onAllUnsubscribed, Func<T, bool> filter)
		{
			this.onFirstSubscriber = onFirstSubscriber;
			this.onAllUnsubscribed = onAllUnsubscribed;
			this.filter = filter;
		}

		public IDisposable Subscribe(IObserver<T> observer)
		{
			bool firstSubscriber;

			lock (gate)
			{
				if (exception != null)
				{
					observer.OnError(exception);
					return new DisposableAction(() => { });
				}

				if (isComplete)
				{
					observer.OnCompleted();
					return new DisposableAction(() => { });
				}

				firstSubscriber = subscribers.Count == 0;
				subscribers = subscribers.Add(observer);
			}

			if (firstSubscriber)
			{
				onFirstSubscriber();
			}

			return new DisposableAction(() =>
			{
				bool lastSubscriber;

				lock (gate)
				{
					subscribers = subscribers.Remove(observer);
					lastSubscriber = subscribers.Count == 0;
				}

				if (lastSubscriber)
					onAllUnsubscribed();
			});
		}

		public override void OnNext(Notification item)
		{
			if (!(item is T) || !filter((T)item))
				return;

			ImmutableList<IObserver<T>> subscribers;
			lock (gate)
			{
				subscribers = this.subscribers;
			}

			foreach (var subscriber in subscribers)
			{
				subscriber.OnNext((T)item);
			}
		}

		public override void OnError(Exception ex)
		{
			ImmutableList<IObserver<T>> subscribers;
			lock (gate)
			{
				if (exception != null)
					return;

				exception = ex;
				subscribers = this.subscribers;
				this.subscribers = new ImmutableList<IObserver<T>>();
			}

			foreach (var observer in subscribers)
			{
				observer.OnError(ex);
			}
		}

		public override void OnCompleted()
		{
			ImmutableList<IObserver<T>> subscribers;
			lock (gate)
			{
				if (isComplete)
					return;

				isComplete = true;
				subscribers = this.subscribers;
				this.subscribers = new ImmutableList<IObserver<T>>();
			}

			foreach (var observer in subscribers)
			{
				observer.OnCompleted();
			}
		}
	}
}
