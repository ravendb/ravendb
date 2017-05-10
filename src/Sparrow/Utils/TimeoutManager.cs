//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections.LockFree;

namespace Sparrow.Utils
{
    public static class TimeoutManager
    {
        private static readonly ConcurrentDictionary<int, TimerTaskHolder> Values 
            = new ConcurrentDictionary<int, TimerTaskHolder>(NumericEqualityComparer.Instance);

        private class TimerTaskHolder  : IDisposable
        {
            private TaskCompletionSource<object> _nextTimeout;
            private readonly Timer _timer;

            private void TimerCallback(object state)
            {
                var old = Interlocked.Exchange(ref _nextTimeout, null);
                old?.TrySetResult(null);
            }

            public Task NextTask
            {
                get
                {
                    while (true)
                    {
                        var tcs = _nextTimeout;
                        if (tcs != null)
                            return tcs.Task;

                        tcs = new TaskCompletionSource<object>();
                        if (Interlocked.CompareExchange(ref _nextTimeout, tcs, null) == null)
                            return tcs.Task;
                    }
                }
            }

            public TimerTaskHolder(int timeout)
            {
                _timer = new Timer(TimerCallback, null, timeout, timeout);
            }

            public void Dispose()
            {
                _timer?.Dispose();
            }
        }

        public static Task WaitFor(int duration)
        {
            return Task.Delay(duration);

            //var mod = duration % 50;
            //if (mod != 0)
            //    duration += 50 - mod;

            //if (Values.TryGetValue(duration, out var value))
            //    return value.NextTask;

            //value = Values.GetOrAdd(duration, d => new TimerTaskHolder(d));
            //return value.NextTask;
        }

        public static  Task WaitFor(int duration, CancellationToken token)
        {
            return Task.Delay(duration, token);
            //token.ThrowIfCancellationRequested();
            //// ReSharper disable once MethodSupportsCancellation
            //var task = WaitFor(duration);
            //if (token == CancellationToken.None || token.CanBeCanceled == false)
            //{
            //    await task;
            //    return;
            //}

            //var onCancel = new TaskCompletionSource<object>();
            //using (token.Register(tcs => onCancel.TrySetCanceled(), onCancel))
            //{
            //    await Task.WhenAny(task, onCancel.Task);
            //}
        }
    }
}
