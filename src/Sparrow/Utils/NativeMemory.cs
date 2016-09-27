using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;

namespace Sparrow.Utils
{
    public static unsafe class NativeMemory
    {
        public static ThreadLocal<ThreadStats> ThreadAllocations = new ThreadLocal<ThreadStats>(
            () => new ThreadStats(), trackAllValues:true);

        public static ConcurrentDictionary<string, long> FileMapping = new ConcurrentDictionary<string, long>();

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

        public static void RegisterFileMapping(string name, long size)
        {
            FileMapping.AddOrUpdate(name, size, (_, old) => old + size);
        }

        public static void UnregisterFileMapping(string name, long size)
        {
            var result = FileMapping.AddOrUpdate(name, size, (_, old) => old - size);
            if (result == 0)
            {
                // shouldn't really happen, but let us be on the safe side
                if (FileMapping.TryRemove(name, out result) && result != 0)
                {
                    RegisterFileMapping(name, result);
                }
            }
        }
    }
}