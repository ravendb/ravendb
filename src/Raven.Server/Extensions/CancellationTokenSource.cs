// -----------------------------------------------------------------------
//  <copyright file="CancellationTokenSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading;

namespace Raven.Server.Extensions
{
    public static class CancellationTokenSourceExtensions
    {
        public static CancellationTimeout TimeoutAfter(this CancellationTokenSource cts, TimeSpan dueTime)
        {
            return new CancellationTimeout(cts, dueTime);
        }
    }

    public class CancellationTimeout : IDisposable
    {
        public CancellationTokenSource CancellationTokenSource { get; private set; }
        private readonly Timer timer;
        private readonly long dueTime;
        private readonly object locker = new object();
        private volatile bool isTimerDisposed;
        private Stopwatch sp = Stopwatch.StartNew();

        public void ThrowIfCancellationRequested()
        {
            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        public CancellationTimeout(CancellationTokenSource cancellationTokenSource, TimeSpan dueTime)
        {
            if (cancellationTokenSource == null)
                throw new ArgumentNullException("cancellationTokenSource");
            if (dueTime < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException("dueTime");

            isTimerDisposed = false;
            CancellationTokenSource = cancellationTokenSource;
            this.dueTime = (long)dueTime.TotalMilliseconds;
            timer = new Timer(self =>
            {
                var source = self as CancellationTokenSource;
                if (source == null)
                    return;

                try
                {
                    source.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }, cancellationTokenSource, (int)this.dueTime, -1);
        }

        ~CancellationTimeout()
        {
            DisposeInternal();
        }

        public void Delay()
        {
            if (isTimerDisposed)
                return;

            if (sp.ElapsedMilliseconds < 500)
                return;

            lock (locker)
            {
                if (isTimerDisposed)
                    return;
                if (sp.ElapsedMilliseconds < 500)
                    return;

                sp.Restart();
                timer.Change((int)dueTime, -1);
            }
        }

        public void Pause()
        {
            if (isTimerDisposed)
                return;

            lock (locker)
            {
                if (isTimerDisposed)
                    return;

                timer.Change(Timeout.Infinite, Timeout.Infinite);
            }
        }

        public void Resume()
        {
            Delay();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            DisposeInternal();
        }

        private void DisposeInternal()
        {
            if (isTimerDisposed)
                return;

            lock (locker)
            {
                if (isTimerDisposed)
                    return;

                isTimerDisposed = true;
            }

            if (timer != null)
                timer.Dispose();
        }
    }
}