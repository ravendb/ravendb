using System;
using System.Threading;

namespace Sparrow.Utils
{
    public static class CurrentProcessorIdHelper
    {
        public static int GetCurrentProcessorId()
        {
#if NETCOREAPP3_1
            return Thread.GetCurrentProcessorId();
#else
            return Thread.CurrentThread.ManagedThreadId % Environment.ProcessorCount;
#endif
        }
    }
}
