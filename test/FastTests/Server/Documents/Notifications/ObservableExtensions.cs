using System;
using System.Collections.Generic;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.Notifications
{
    public static class ObservableExtensions
    {
        private class ActionObserver<T> : IObserver<T>
        {
            private readonly Action<T> _action;

            public ActionObserver(Action<T> action)
            {
                _action = action;
            }

            public void OnCompleted()
            {
            }

            public void OnError(Exception error)
            {
            }

            public void OnNext(T value)
            {
                _action(value);
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
    }
}