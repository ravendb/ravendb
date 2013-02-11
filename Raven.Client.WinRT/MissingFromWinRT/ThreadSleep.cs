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
			new ManualResetEvent(false).WaitOne(ms);
#else
			Thread.Sleep(ms);
#endif
		}
	}
}