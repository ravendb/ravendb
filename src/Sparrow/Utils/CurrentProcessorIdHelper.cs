using System;
using System.Threading;

namespace Sparrow.Utils
{
    internal static class CurrentProcessorIdHelper
    {
        public static int GetCurrentProcessorId()
        {
#if NETSTANDARD2_0
            return Thread.CurrentThread.ManagedThreadId % Environment.ProcessorCount;
#else
            return Thread.GetCurrentProcessorId();
#endif
        }
    }
}
