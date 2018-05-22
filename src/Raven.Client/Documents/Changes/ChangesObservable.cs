using System;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Collections;

namespace Raven.Client.Documents.Changes
{
    internal class ChangesObservable<T, TConnectionState> : IChangesObservable<T> where TConnectionState : IChangesConnectionState<T>
    {
        private readonly TConnectionState _connectionState;
        private readonly Func<T, bool> _filter;
        private readonly ConcurrentSet<IObserver<T>> _subscribers = new ConcurrentSet<IObserver<T>>();

        internal ChangesObservable(
            TConnectionState connectionState,
            Func<T, bool> filter)
        {
            _connectionState = connectionState;
            _filter = filter;
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            _connectionState.OnChangeNotification += Send;
            _connectionState.OnError += Error;

            _connectionState.Inc();
            _subscribers.TryAdd(observer);
            return new DisposableAction(() =>
            {
                _connectionState.Dec();
                _subscribers.TryRemove(observer);

                _connectionState.OnChangeNotification -= Send;
                _connectionState.OnError -= Error;
            });
        }

        public void Send(T msg)
        {
            try
            {
                if (_filter(msg) == false)
                    return;
            }
            catch (Exception e)
            {
                Error(e);
                return;
            }

            foreach (var subscriber in _subscribers)
            {
                subscriber.OnNext(msg);
            }
        }

        public void Error(Exception e)
        {
            foreach (var subscriber in _subscribers)
            {
                subscriber.OnError(e);
            }
        }

        public Task EnsureSubscribedNow()
        {
            return _connectionState.EnsureSubscribedNow();
        }
    }
}
