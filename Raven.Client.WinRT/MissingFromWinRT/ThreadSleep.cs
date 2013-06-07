// -----------------------------------------------------------------------
//  <copyright file="ThreadSleep.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public class ThreadSleep
	{
		public static void Sleep(int ms)
		{
#if NETFX_CORE
			using(var e = new ManualResetEvent(false))
				e.WaitOne(ms);
#else
			Thread.Sleep(ms);
#endif
		}
	}
}