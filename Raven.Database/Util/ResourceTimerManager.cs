// -----------------------------------------------------------------------
//  <copyright file="ResourceTimerManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;

using Raven.Abstractions.Logging;
using Raven.Database.Impl;

namespace Raven.Database.Util
{
	public class ResourceTimerManager : IDisposable
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly IList<Timer> timers = new List<Timer>();

		public void ExecuteTimer(TimerCallback callback, TimeSpan dueTime, TimeSpan period)
		{
			GetTimer(callback, dueTime, period);
		}

		public Timer GetTimer(TimerCallback callback, TimeSpan dueTime, TimeSpan period)
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
				aggregator.Execute(() =>
				{
					if (timer != null)
						timer.Dispose();
				});
			}

			aggregator.ThrowIfNeeded();
		}

		public void ReleaseTimer(Timer timer)
		{
			if (timer == null)
				throw new ArgumentNullException("timer");

			if (timers.Remove(timer) == false)
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