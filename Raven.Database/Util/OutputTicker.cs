// -----------------------------------------------------------------------
//  <copyright file="OutputTicker.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Timers;

namespace Raven.Database.Util
{
	public class OutputTicker : IDisposable
	{
		public OutputTicker(TimeSpan interval, Action output, Action onStart = null, Action onStop = null)
		{
			timer = new Timer
			{
				Interval = interval.TotalMilliseconds
			};

			timer.Elapsed += (sender, args) => output();

			this.onStart = onStart;
			this.onStop = onStop;
		}

		private readonly Timer timer;

		private readonly Action onStart;

		private readonly Action onStop;

		public void Start()
		{
			if (onStart != null)
				onStart();

			timer.Start();
		}

		public void Stop()
		{
			timer.Stop();

			if (onStop != null)
				onStop();
		}

		public void Dispose()
		{
			if (timer != null)
				timer.Dispose();
		}
	}
}