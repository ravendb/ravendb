// -----------------------------------------------------------------------
//  <copyright file="CancellationTokenSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

namespace Raven.Database.Extensions
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

        public void ThrowIfCancellationRequested()
        {
            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        public CancellationTimeout(CancellationTokenSource source, TimeSpan dueTime)
        {
            if (source == null)
                throw new ArgumentNullException("source");
            if (dueTime < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException("dueTime");

            isTimerDisposed = false;
            CancellationTokenSource = source;
            this.dueTime = (long)dueTime.TotalMilliseconds;
            timer = new Timer(self =>
            {
                Dispose(); 

                try
                {
                    CancellationTokenSource.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }, null, this.dueTime, -1);
        }

	    ~CancellationTimeout()
	    {
			DisposeInternal();
	    }

	    public void Delay()
        {
			if (isTimerDisposed)
				return;

	        lock (locker)
	        {
		        if (isTimerDisposed) 
					return;

				timer.Change(dueTime, -1);
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

				if (timer != null)
					timer.Dispose();
			}
	    }
    }
}