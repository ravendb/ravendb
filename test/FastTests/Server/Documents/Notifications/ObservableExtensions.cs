using System;

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

        public static IDisposable Subscribe<T>(this IObservable<T> self, Action<T> action)
        {
            return self.Subscribe(new ActionObserver<T>(action));
        }
    }
}