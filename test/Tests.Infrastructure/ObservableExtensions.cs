using System;
using System.Collections.Generic;
using Raven.Client.Util;

namespace FastTests
{
    public static class ObservableExtensions
    {
        private class ActionObserver<T> : IObserver<T>
        {
            private readonly Action<T> _onNext;
            private readonly Action<Exception> _onError;
            private readonly Action _onCompleted;

            public ActionObserver(Action<T> onNext, Action<Exception> onError = null, Action onCompleted = null)
            {
                _onNext = onNext;
                _onError = onError;
                _onCompleted = onCompleted;
            }

            public void OnCompleted()
            {
                _onCompleted?.Invoke();
            }

            public void OnError(Exception error)
            {
                _onError?.Invoke(error);
            }

            public void OnNext(T value)
            {
                _onNext(value);
            }
        }

        public class FilteredObserver<T> : IObserver<T>
        {
            private readonly Func<T, bool> _predicate;
            private List<Action<T>> _subscribers = new List<Action<T>>();

            public FilteredObserver(Func<T, bool> predicate)
            {
                _predicate = predicate;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(T value)
            {
                if (_predicate(value))
                {
                    foreach (var subscriber in _subscribers)
                    {
                        subscriber(value);
                    }
                }
            }

            public IDisposable Subscribe(Action<T> action)
            {
                _subscribers.Add(action);
                return new DisposableAction(() =>
                {
                    _subscribers.Remove((action));
                });
            }
        }

        public static FilteredObserver<T> Where<T>(this IObservable<T> self, Func<T, bool> predicate)
        {
            var filteredObserver = new FilteredObserver<T>(predicate);
            self.Subscribe(filteredObserver);
            return filteredObserver;
        }

        public static IDisposable Subscribe<T>(this IObservable<T> self, Action<T> action)
        {
            return self.Subscribe(new ActionObserver<T>(action));
        }

        public static IDisposable Subscribe<T>(this IObservable<T> self, Action<T> onNext, Action<Exception> onError)
        {
            return self.Subscribe(new ActionObserver<T>(onNext, onError));
        }

        public static IDisposable Subscribe<T>(this IObservable<T> self, Action<T> onNext, Action<Exception> onError, Action onCompleted)
        {
            return self.Subscribe(new ActionObserver<T>(onNext, onError, onCompleted));
        }
    }
}
