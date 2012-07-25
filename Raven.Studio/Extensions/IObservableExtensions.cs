using System;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Net;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
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

        public static IObservable<EventPattern<NotifyCollectionChangedEventArgs>> ObserveCollectionChanged(this INotifyCollectionChanged source)
        {
            return Observable.FromEventPattern<NotifyCollectionChangedEventHandler, NotifyCollectionChangedEventArgs>(
                h => (sender, e) => h(sender, e),
                h => source.CollectionChanged += h,
                h => source.CollectionChanged -= h);
        } 

        /// <summary>
        /// Samples an event stream such that the very first event is reported, but then no further events
        /// are reported until a Timespan of delay has elapsed, at which point the most recent event will be reported. And so on.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="delay"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Only connects to source when subscribed to, and disconnects when there are no subscribers, allowing a delay to ensure
        /// no one else subscribes in the meantime
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="cleanupDelay"></param>
        /// <returns></returns>
        public static IObservable<TSource> DelayedCleanupRefCount<TSource>(this IConnectableObservable<TSource> source, TimeSpan cleanupDelay)
        {
            if (source == null)
                throw new ArgumentNullException("source");
            object gate = new object();
            int count = 0;
            IDisposable connectableSubscription = null;
            return Observable.Create(((Func<IObserver<TSource>, IDisposable>)(observer =>
            {
                bool isFirst = false;
                lock (gate)
                {
                    ++count;
                    isFirst = count == 1;
                }
                IDisposable subscription = source.Subscribe(observer);
                if (isFirst)
                    connectableSubscription = source.Connect();

                return Disposable.Create((Action)(() =>
                {
                    bool isLast = false;
                    subscription.Dispose();
                    lock (gate)
                    {
                        --count;
                        isLast = count == 0;
                    }

                    if (isLast)
                    {
                        Scheduler.ThreadPool.Schedule(cleanupDelay, () =>
                        {
                            lock (gate)
                            {
                                // only dispose the connectable subscription if no one else has subscribed in the meantime
                                if (count == 0)
                                {
                                    connectableSubscription.Dispose();
                                }
                            }
                        });
                    }
                }));
            })));
        }
    }
}
