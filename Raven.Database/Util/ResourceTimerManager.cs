// -----------------------------------------------------------------------
//  <copyright file="ResourceTimerManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;

using Raven.Abstractions.Logging;
using Raven.Database.Impl;

namespace Raven.Database.Util
{
	public class ResourceTimerManager : IDisposable
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly ConcurrentSet<Timer> timers = new ConcurrentSet<Timer>();

		public Timer NewTimer(TimerCallback callback, TimeSpan dueTime, TimeSpan period)
		{
			var timer = new Timer(callback, null, dueTime, period);
			timers.Add(timer);

			return timer;
		}

		public void Dispose()
		{
			var aggregator = new ExceptionAggregator("Error during ResourceTimerManager disposal.");

			foreach (var timer in timers)
			{
				var t = timer;
				aggregator.Execute(() =>
				{
					if (t != null)
						t.Dispose();
				});
			}

			aggregator.ThrowIfNeeded();
		}

		public void ReleaseTimer(Timer timer)
		{
			if (timer == null)
				throw new ArgumentNullException("timer");

			if (timers.TryRemove(timer) == false)
				throw new InvalidOperationException("Unknown timer.");

			try
			{
				timer.Dispose();
			}
			catch (Exception e)
			{
				log.WarnException("Timer was not disposed correctly.", e);
			}
		}
	}
}