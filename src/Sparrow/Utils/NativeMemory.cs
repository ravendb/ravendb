using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.LowMemory;
using Sparrow.Threading;

namespace Sparrow.Utils
{
    public static unsafe class NativeMemory
    {
        public static Func<ulong> GetCurrentUnmanagedThreadId = () => 0xDEAD;

        internal static readonly LightWeightThreadLocal<ThreadStats> ThreadAllocations = new LightWeightThreadLocal<ThreadStats>(
            () => new ThreadStats());

        public static ThreadStats GetByThreadId(int threadId)
        {
            return ThreadAllocations.Values.FirstOrDefault(x => x != null && x.ManagedThreadId == threadId);
        } 
        
        public static void NotifyCurrentThreadAboutToClose()
        {
            ThreadAllocations.Value = null;
        }

        public static ThreadStats CurrentThreadStats => ThreadAllocations.Value;

        public static IEnumerable<ThreadStats> AllThreadStats
        {
            get
            {
                foreach (var threadStats in ThreadAllocations.Values)
                {
                    if (threadStats != null)
                        yield return threadStats;
                }
            }
        }

        internal static long _totalAllocatedMemory;
        private static long _totalLuceneManagedAllocationsForTermCache;
        private static long _totalLuceneUnmanagedAllocationsForSorting;

        public static long TotalAllocatedMemory => _totalAllocatedMemory;
        public static long TotalLuceneManagedAllocationsForTermCache => _totalLuceneManagedAllocationsForTermCache;
        public static long TotalLuceneUnmanagedAllocationsForSorting => _totalLuceneUnmanagedAllocationsForSorting;

        public static ConcurrentDictionary<string, Lazy<FileMappingInfo>> FileMapping = new ConcurrentDictionary<string, Lazy<FileMappingInfo>>();

        public sealed class ThreadStats
        {
            public int InternalId;
            public ulong UnmanagedThreadId;
            public long Allocations;
            public long ReleasesFromOtherThreads;
            private WeakReference<Thread> _threadInstance = new WeakReference<Thread>(null);
            public readonly int ManagedThreadId;
            private string _lastName = "Unknown";

            internal string CapturedStackTrace;

            public string Name
            {
                get
                {
                    var threadInstance = _threadInstance;
                    if (threadInstance != null && threadInstance.TryGetTarget(out var thread))
                    {
                        return thread?.Name ?? _lastName;
                    }

                    return _lastName;
                }
            }
            public long TotalAllocated => Allocations - ReleasesFromOtherThreads;

            private static int _uniqueThreadId = 0;

            public long CurrentlyAllocatedForProcessing;

            public bool IsThreadAlive()
            {
                var threadInstance = _threadInstance;
                if (threadInstance == null)
                    return false;

                if (threadInstance.TryGetTarget(out var copy) == false)
                    return false;

                if (copy == null)
                    return false;

                if (copy.IsAlive)
                    return true;

                _threadInstance = null; // intentionally not thread safe, worst case it will take time to see this
                _lastName = copy.Name; // fine if multiple threads setting this

                return false;
            }

            private ThreadStats(object marker) { }

            public static ThreadStats Empty = new ThreadStats(null);

            public ThreadStats()
            {
                var currentThread = Thread.CurrentThread;
                _threadInstance.SetTarget(currentThread);
                ManagedThreadId = currentThread.ManagedThreadId;
                InternalId = Interlocked.Increment(ref _uniqueThreadId);
                UnmanagedThreadId = GetCurrentUnmanagedThreadId.Invoke();
            }
        }

        public static void Free(byte* ptr, long size, ThreadStats stats)
        {
            Debug.Assert(ptr != null);

            UpdateMemoryStatsForThread(stats, size);
            Interlocked.Add(ref _totalAllocatedMemory, -size);

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

            // Allocating when there isn't enough commit charge available is dangerous, on Linux, the OOM
            // will try to kill us. On Windows, we might get into memory allocation failures that are not
            // fun, so let's try to avoid it explicitly.
            // This is not expected to be called frequently, since we are caching the memory used here

            LowMemoryNotification.AssertNotAboutToRunOutOfMemory();

            try
            {
                var ptr = (byte*)Marshal.AllocHGlobal((IntPtr)size).ToPointer();
                thread.Allocations += size;
                Interlocked.Add(ref _totalAllocatedMemory, size);
                return ptr;
            }
            catch (OutOfMemoryException e)
            {
                return ThrowFailedToAllocate(size, thread, e);
            }
        }

        public static void IncrementLuceneManagedAllocations(long size)
        {
            Interlocked.Add(ref _totalLuceneManagedAllocationsForTermCache, size);
        }

        public static void DecrementLuceneManagedAllocations(long size)
        {
            Interlocked.Add(ref _totalLuceneManagedAllocationsForTermCache, -size);
        }

        public static byte* AllocateMemoryByLucene(long size)
        {
            ThreadStats _;
            Interlocked.Add(ref _totalLuceneUnmanagedAllocationsForSorting, size);
            return AllocateMemory(size, out _);
        }

        public static void FreeMemoryByLucene(byte* ptr, long size)
        {
            Interlocked.Add(ref _totalLuceneUnmanagedAllocationsForSorting, -size);
            Free(ptr, size, ThreadAllocations.Value);
        }

        private static byte* ThrowFailedToAllocate(long size, ThreadStats thread, OutOfMemoryException e)
        {
            long allocated = 0;
            foreach (var threadAllocationsValue in AllThreadStats)
            {
                allocated += threadAllocationsValue.TotalAllocated;
            }

            var managed = AbstractLowMemoryMonitor.GetManagedMemoryInBytes();
            var unmanagedMemory = AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes();

            throw new OutOfMemoryException($"Failed to allocate additional {new Size(size, SizeUnit.Bytes)} " +
                                           $"to already allocated {new Size(thread.TotalAllocated, SizeUnit.Bytes)} by this thread. " +
                                           $"Total allocated by all threads: {new Size(allocated, SizeUnit.Bytes)}, " +
                                           $"Managed memory: {new Size(managed, SizeUnit.Bytes)}, " +
                                           $"Un-managed memory: {new Size(unmanagedMemory, SizeUnit.Bytes)}", e);
        }

        internal static void UpdateMemoryStatsForThread(ThreadStats stats, long size)
        {
            var currentThreadValue = ThreadAllocations.Value;
            if (currentThreadValue == stats)
            {
                currentThreadValue.Allocations -= size;

                // fix allocations with releases from other threads
                var released = currentThreadValue.ReleasesFromOtherThreads;
                if (released > 0)
                {
                    currentThreadValue.Allocations -= released;
                    Interlocked.Add(ref currentThreadValue.ReleasesFromOtherThreads, -released);
                }
            }
            else
            {
                Interlocked.Add(ref stats.ReleasesFromOtherThreads, size);
            }
        }

        public static void RegisterFileMapping(string fullPath, IntPtr start, long size, Func<long> getAllocatedSize)
        {
            var lazyMapping = FileMapping.GetOrAdd(fullPath, _ =>
            {
                return new Lazy<FileMappingInfo>(() =>
                {
                    var fileType = GetFileType(fullPath);
                    return new FileMappingInfo
                    {
                        FileType = fileType
                    };
                });
            });

            lazyMapping.Value.GetAllocatedSizeFunc = getAllocatedSize;
            lazyMapping.Value.Info.TryAdd(start, size);
        }

        private static FileType GetFileType(string fullPath)
        {
            var extension = Path.GetExtension(fullPath);
            if (extension == null)
                return FileType.Data;

            if (extension.Equals(".buffers", StringComparison.OrdinalIgnoreCase) == false)
                return FileType.Data;

            var fileName = Path.GetFileName(fullPath);
            if (fileName == null)
                return FileType.ScratchBuffer;

            if (fileName.StartsWith("scratch", StringComparison.OrdinalIgnoreCase))
                return FileType.ScratchBuffer;

            if (fileName.StartsWith("compression", StringComparison.OrdinalIgnoreCase))
                return FileType.CompressionBuffer;

            if (fileName.StartsWith("decompression", StringComparison.OrdinalIgnoreCase))
                return FileType.DecompressionBuffer;

            return FileType.Data;
        }

        public static void UnregisterFileMapping(string name)
        {
            FileMapping.TryRemove(name, out _);
        }

        public static void UnregisterFileMapping(string name, IntPtr start, long size)
        {
            if (FileMapping.TryGetValue(name, out var mapping) == false)
                return;

            var info = mapping.Value.Info;
            info.TryRemove(start, out _);
            if (info.Count > 0)
                return;

            if (FileMapping.TryRemove(name, out var value) == false)
                return;

            if (value.Value.Info.Count > 0) // this shouldn't happen, but let us be on the safe side...
            {
                FileMapping.TryAdd(name, value);
            }
        }

        public static void EnsureRegistered()
        {
            GC.KeepAlive(ThreadAllocations.Value); // side affecty
        }

        public sealed class FileMappingInfo
        {
            public FileMappingInfo()
            {
                Info = new ConcurrentDictionary<IntPtr, long>();
            }

            public ConcurrentDictionary<IntPtr, long> Info { get; set; }

            public Func<long> GetAllocatedSizeFunc { get; set; }

            public FileType FileType { get; set; }
        }

        public enum FileType
        {
            Data,
            ScratchBuffer,
            CompressionBuffer,
            DecompressionBuffer
        }
    }
}
