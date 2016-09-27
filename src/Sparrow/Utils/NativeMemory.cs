using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace Sparrow.Utils
{
    public static unsafe class NativeMemory
    {
        public static ThreadLocal<ThreadStats> ThreadAllocations = new ThreadLocal<ThreadStats>(
            () => new ThreadStats(), trackAllValues:true);

        public class ThreadStats
        {
            public string Name;
            public int Id;
            public long Allocations;

            public ThreadStats()
            {
                var currentThread = Thread.CurrentThread;
                Name = currentThread.Name;
                Id = currentThread.ManagedThreadId;
            }
        }

        public static void Free(byte* ptr, long size)
        {
            ThreadAllocations.Value.Allocations -= size;
            Marshal.FreeHGlobal((IntPtr)ptr);
        }

        public static byte* AllocateMemory(long size)
        {
            ThreadAllocations.Value.Allocations += size;

            return (byte*)Marshal.AllocHGlobal((IntPtr)size).ToPointer();
        }
    }
}