using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Collections;

namespace Raven.Client.Documents.Changes
{
    internal sealed class ChangesObservable<T, TConnectionState> : IChangesObservable<T> where TConnectionState : IChangesConnectionState<T>
    {
        private readonly TConnectionState _connectionState;
        private readonly Func<T, bool> _filter;
        private ConcurrentSet<IObserver<T>> _subscribers = new ConcurrentSet<IObserver<T>>();

        internal ChangesObservable(
            TConnectionState connectionState,
            Func<T, bool> filter)
        {
            _connectionState = connectionState;
            _filter = filter;
        }

        private bool TryRegisterFirstObserver(IObserver<T> observer)
        {
            var current = _subscribers;
            if (current.IsEmpty)
            {
                var firstConnection = new ConcurrentSet<IObserver<T>> { observer };
                return Interlocked.CompareExchange(ref _subscribers, firstConnection, current) == current;
            }

            return false;
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (TryRegisterFirstObserver(observer))
            {
                _connectionState.Inc();
                // first subscriber, register to the connection state events
                _connectionState.OnChangeNotification += Send;
                _connectionState.OnError += Error;
            }
            else if (_subscribers.TryAdd(observer))
            {
                // the first subscriber, has already registered to the connection state events, we only Inc the count
                _connectionState.Inc();
            }


            return new DisposableAction(() => DisposeInternal(observer));
        }

        private void DisposeInternal(IObserver<T> observer)
        {
            if (_subscribers.TryRemove(observer) == false)
                return;

            var count = _connectionState.Dec();
            if (count == 0)
            {
                // last subscriber gone, unregister from the connection state events
                _connectionState.OnChangeNotification -= Send;
                _connectionState.OnError -= Error;
            }
        }

        public void Send(T msg)
        {
            if (_filter != null)
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
