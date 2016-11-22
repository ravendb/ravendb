using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Client.Helpers;

using Sparrow.Collections;

namespace Raven.NewClient.Client.Util
{
    public class SimpleCache : IDisposable
    {
        private readonly ConcurrentLruSet<string> lruKeys;
        private readonly ConcurrentDictionary<string, CachedRequest> actualCache;

        private readonly ConcurrentDictionary<string, int> lastWritePerDb = new ConcurrentDictionary<string, int>();

        public SimpleCache(int maxNumberOfCacheEntries)
        {
            actualCache = new ConcurrentDictionary<string, CachedRequest>();
            lruKeys = new ConcurrentLruSet<string>(maxNumberOfCacheEntries, key =>
            {
                CachedRequest _;
                actualCache.TryRemove(key, out _);
            });
        }

        static readonly bool runningOnMono = Type.GetType("Mono.Runtime") != null;
        private static bool failedToGetAvailablePhysicalMemory;

        private static bool RunningOnMono
        {
            get { return runningOnMono; }
        }

        private static int AvailableMemory
        {
            get
            {
                if (failedToGetAvailablePhysicalMemory)
                    return -1;

                if (RunningOnMono)
                {
                    // Try /proc/meminfo, which will work on Linux only!
                    if (File.Exists("/proc/meminfo"))
                    {
                        using (TextReader reader = File.OpenText("/proc/meminfo"))
                        {
                            var match = Regex.Match(reader.ReadToEnd(), @"MemFree:\s*(\d+) kB");
                            if (match.Success)
                            {
                                return Convert.ToInt32(match.Groups[1].Value) / 1024;
                            }
                        }
                    }
                    failedToGetAvailablePhysicalMemory = true;
                    return -1;
                }
#if __MonoCS__ || MONO
                throw new PlatformNotSupportedException("This build can only run on Mono");
#else
                try
                {
                    var availablePhysicalMemoryInMb = (int)(EnvironmentHelper.AvailablePhysicalMemory / 1024 / 1024);
                    if (EnvironmentHelper.Is64BitProcess)
                    {
                        return availablePhysicalMemoryInMb;
                    }

                    // we are in 32 bits mode, but the _system_ may have more than 4 GB available
                    // so we have to check the _address space_ as well as the available memory
                    // 32bit processes are limited to 1.5GB of heap memory
                    var workingSetMb = (int)(Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024);
                    return Math.Min(1536 - workingSetMb, availablePhysicalMemoryInMb);
                }
                catch
                {
                    failedToGetAvailablePhysicalMemory = true;
                    return -1;
                }
#endif
            }
        }

        private int memoryPressureCounterOnSet, memoryPressureCounterOnGet;

        public void Set(string key, CachedRequest val)
        {
            if (Interlocked.Increment(ref memoryPressureCounterOnSet) % 25 == 0) // check every 25 sets
            {
                TryClearMemory();
            }
            int lastWrite;
            if (lastWritePerDb.TryGetValue(val.Database, out lastWrite))
            {
                val.ReadTime = lastWrite;
            }
            actualCache.AddOrUpdate(key, val, (s, o) => val);
            lruKeys.Push(key);
        }

        private void TryClearMemory()
        {
            var availableMemory = AvailableMemory;
            if (AvailableMemory != -1 && availableMemory < 1024) // clear the cache if there is low memory
            {
                lruKeys.ClearHalf();
            }
        }

        public CachedRequest Get(string key)
        {
            CachedRequest value;
            if (actualCache.TryGetValue(key, out value))
            {
                lruKeys.Push(key);
                if (Interlocked.Increment(ref memoryPressureCounterOnGet) % 1000 == 0) // check every 1000 gets
                {
                    TryClearMemory();
                }
            }
            if (value != null)
            {
                int lastWrite;
                if (lastWritePerDb.TryGetValue(value.Database, out lastWrite))
                {
                    if (value.ReadTime < lastWrite)
                        value.ForceServerCheck = true;
                }
            }
            return value;
        }

        public int CurrentSize
        {
            get { return actualCache.Count; }
        }

        public void Dispose()
        {
            lruKeys.Clear();
            actualCache.Clear();
        }

        internal void ForceServerCheckOfCachedItemsForDatabase(string databaseName)
        {
            lastWritePerDb.AddOrUpdate(databaseName, 1, (db, existingTime) => existingTime + 1);
        }
    }
}
