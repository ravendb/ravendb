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

        public static ConcurrentDictionary<string, ConcurrentDictionary<IntPtr, long>> FileMapping = new ConcurrentDictionary<string, ConcurrentDictionary<IntPtr, long>>();

        public class ThreadStats
        {
            public string Name;
            public int Id;
            public long Allocations;
            public long ReleasesFromOtherThreads;
            public Thread ThreadInstance;

            public ThreadStats()
            {
                ThreadInstance = Thread.CurrentThread;
                Name = ThreadInstance.Name;
                Id = ThreadInstance.ManagedThreadId;
            }
        }
        public static void Free(byte* ptr, long size, ThreadStats stats)
        {
            var currentThreadValue = ThreadAllocations.Value;
            if (currentThreadValue == stats)
            {
                currentThreadValue.Allocations -= size;
                FixupReleasesFromOtherThreads(currentThreadValue);
            }
            else
            {
                Interlocked.Add(ref stats.ReleasesFromOtherThreads, size);
            }
            Marshal.FreeHGlobal((IntPtr)ptr);
        }

        public static void Free(byte* ptr, long size)
        {
            Free(ptr, size, ThreadAllocations.Value);
        }

        public static byte* AllocateMemory(long size)
        {
            ThreadStats _;
            return AllocateMemory(size, out _);
        }

        public static byte* AllocateMemory(long size, out ThreadStats thread)
        {
            thread = ThreadAllocations.Value;
            thread.Allocations += size;

            return (byte*)Marshal.AllocHGlobal((IntPtr)size).ToPointer();
        }

        private static void FixupReleasesFromOtherThreads(ThreadStats thread)
        {
            var released = thread.ReleasesFromOtherThreads;
            if (released > 0)
            {
                thread.Allocations -= released;
                Interlocked.Add(ref thread.ReleasesFromOtherThreads, -released);
            }
        }

        public static void RegisterFileMapping(string name, IntPtr start, long size)
        {
            var mapping = FileMapping.GetOrAdd(name,_ => new ConcurrentDictionary<IntPtr, long>());
            mapping.TryAdd(start, size);
        }

        public static void UnregisterFileMapping(string name, IntPtr start, long size)
        {
            ConcurrentDictionary<IntPtr, long> mapping;
            if (FileMapping.TryGetValue(name, out mapping) == false)
                return;

            long _;
            mapping.TryRemove(start, out _);
            if (mapping.Count == 0)
            {
                ConcurrentDictionary<IntPtr, long> value;
                if (FileMapping.TryRemove(name, out value))
                {
                    if (value.Count > 0) // this shouldn't happen, but let us be on the safe side...
                    {
                        FileMapping.TryAdd(name, value);
                    }
                }
            }
        }
    }
}