using System;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.NewClient.Database.Util;
using Sparrow.Collections;

namespace Raven.NewClient.Client.Changes
{
    public class TaskedObservable<T, TConnectionState> : IObservableWithTask<T> where TConnectionState : IChangesConnectionState
    {
        private readonly TConnectionState localConnectionState;
        private readonly Func<T, bool> filter;
        private readonly ConcurrentSet<IObserver<T>> subscribers = new ConcurrentSet<IObserver<T>>();

        internal TaskedObservable(
            TConnectionState localConnectionState, 
            Func<T, bool> filter)
        {
            this.localConnectionState = localConnectionState;
            this.filter = filter;
            Task = localConnectionState.Task.ContinueWith(task =>
            {
                task.AssertNotFailed();
                return (IObservable<T>)this;
            });
        }

        public Task<IObservable<T>> Task { get; private set; }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            localConnectionState.Inc();
            subscribers.TryAdd(observer);
            return new DisposableAction(() =>
            {
                localConnectionState.Dec();
                subscribers.TryRemove(observer);
            });
        }

        public void Send(T msg)
        {
            try
            {
                if (filter(msg) == false)
                    return;
            }
            catch (Exception e)
            {
                Error(e);
                return;
            }

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
