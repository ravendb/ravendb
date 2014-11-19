//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;

namespace Raven.Abstractions
{
	public static class SystemTime
	{
        public static readonly double MicroSecPerTick =
            1000000D / System.Diagnostics.Stopwatch.Frequency;

		public static Func<DateTime> UtcDateTime;

	    public static Action<int> WaitCalled; 

		public static DateTime UtcNow
		{
			get
			{
				var temp = UtcDateTime;
				return temp == null ? DateTime.UtcNow : temp();
			}
		}

	    public static void Wait(int durationMs)
	    {
	        var waitCalled = WaitCalled;
	        if (waitCalled != null)
	        {
	            waitCalled(durationMs);
	            return;
	        }
            Thread.Sleep(durationMs);
	    }
	}
}