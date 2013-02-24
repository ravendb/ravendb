using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using Raven.Client.Connection.Profiling;

namespace Raven.Client.Util
{
	public class SimpleCache<T> : IDisposable
	{
		readonly ConcurrentLruLSet<string> lruKeys;
		internal readonly ConcurrentDictionary<string, T> actualCache;

		public SimpleCache(int maxNumberOfCacheEntries)
		{
			actualCache = new ConcurrentDictionary<string, T>();
			lruKeys = new ConcurrentLruLSet<string>(maxNumberOfCacheEntries, key =>
			{
				T _;
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
#if !SILVERLIGHT
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
#if __MonoCS__
				throw new PlatformNotSupportedException("This build can only run on Mono");
#else
				try
				{
					var availablePhysicalMemoryInMb = (int)(new Microsoft.VisualBasic.Devices.ComputerInfo().AvailablePhysicalMemory / 1024 / 1024);
					if (Environment.Is64BitProcess)
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
#else
				return Math.Max(0, 1024 - (int)(GC.GetTotalMemory(false) / 1024 / 1024));

#endif

			}
		}

		private int memoryPressureCounterOnSet, memoryPressureCounterOnGet;

		public void Set(string key, T val)
		{
			if (Interlocked.Increment(ref memoryPressureCounterOnSet) % 25 == 0) // check every 25 sets
			{
				TryClearMemory();
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

		public T Get(string key)
		{
			T value;
			if (actualCache.TryGetValue(key, out value))
			{
				lruKeys.Push(key);
				if (Interlocked.Increment(ref memoryPressureCounterOnGet) % 1000 == 0) // check every 1000 gets
				{
					TryClearMemory();
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
	}
}