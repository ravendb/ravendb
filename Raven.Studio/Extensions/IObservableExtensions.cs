using System;
using System.ComponentModel;
using System.Net;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Extensions
{
    public static class IObservableExtensions
    {
        public static IDisposable SubscribeWeakly<T, TTarget>(this IObservable<T> observable, TTarget target, Action<TTarget, T> onNext) where TTarget : class
        {
            var reference = new WeakReference(target);

            if (onNext.Target != null)
            {
                throw new ArgumentException("onNext must refer to a static method, or else the subscription will still hold a strong reference to target");
            }

            IDisposable subscription = null;
            subscription = observable.Subscribe(item =>
            {
                var currentTarget = reference.Target as TTarget;
                if (currentTarget != null)
                {
                    onNext(currentTarget, item);
                }
                else
                {
                    subscription.Dispose();
                }
            });

            return subscription;
        }

        public static IObservable<EventPattern<PropertyChangedEventArgs>> ObservePropertyChanged(this INotifyPropertyChanged source)
        {
            return Observable.FromEventPattern<PropertyChangedEventHandler, PropertyChangedEventArgs>(
                h => (sender, e) => h(sender, e),
                h => source.PropertyChanged += h,
                h => source.PropertyChanged -= h);
        } 

        public static IObservable<T> SampleResponsive<T>(this IObservable<T> source, TimeSpan delay)
        {
            // code from http://stackoverflow.com/questions/3211134/how-to-throttle-event-stream-using-rx/3224723#3224723
            return source.Publish(src =>
            {
                var fire = new Subject<T>();

                var whenCanFire = fire
                    .Select(u => new Unit())
                    .Delay(delay)
                    .StartWith(new Unit());

                var subscription = src
                    .CombineVeryLatest(whenCanFire, (x, flag) => x)
                    .Subscribe(fire);

                return fire.Finally(subscription.Dispose);
            });
        }

        public static IObservable<TResult> CombineVeryLatest<TLeft, TRight, TResult>(this IObservable<TLeft> leftSource, IObservable<TRight> rightSource, Func<TLeft, TRight, TResult> selector)
        {
            // code from http://stackoverflow.com/questions/3211134/how-to-throttle-event-stream-using-rx/3224723#3224723
            return Observable.Defer(() =>
            {
                int l = -1, r = -1; // the last yielded index from each sequence
                return Observable.CombineLatest(
                    leftSource.Select(Tuple.Create<TLeft, int>), // create a tuple which marks each item in a sequence with its index
                    rightSource.Select(Tuple.Create<TRight, int>),
                        (x, y) => new { x, y })
                    .Where(t => t.x.Item2 != l && t.y.Item2 != r) // don't yield a pair if the left or right has already been yielded
                    .Do(t => { l = t.x.Item2; r = t.y.Item2; }) // record the index of the last item yielded from each sequence
                    .Select(t => selector(t.x.Item1, t.y.Item1));
            });
        }
    }
}
