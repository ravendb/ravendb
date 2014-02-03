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

		public class CancellationTimeout : IDisposable
		{
			private readonly CancellationTokenSource source;
			private readonly Timer timer;
			private readonly long dueTime;
			private bool isTimerDisposed;


			public CancellationTimeout(CancellationTokenSource source, TimeSpan dueTime)
			{
				if (source == null)
					throw new ArgumentNullException("source");
				if (dueTime < TimeSpan.Zero)
					throw new ArgumentOutOfRangeException("dueTime");

				isTimerDisposed = true;
				this.source = source;
				this.dueTime = (long) dueTime.TotalMilliseconds;
				timer = new Timer(self =>
				{
					timer.Dispose();
					isTimerDisposed = true;
					try
					{
						this.source.Cancel();
					}
					catch (ObjectDisposedException)
					{
					}
				}, null, this.dueTime, -1);
			}

			public void Delay()
			{
				if(!isTimerDisposed)
					timer.Change(dueTime, -1);
			}

			public void Dispose()
			{
				timer.Dispose();
			}
		}

	}
}