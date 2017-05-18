using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace Raven.Server.Utils
{
    public static class ThreadStatUtils
    {
        internal static IEnumerable<Thread> GetThreadsOfCurrentProcess()
        {
            foreach (Thread t in Process.GetCurrentProcess().Threads)
                yield return t;
        }
    }
}
