using System;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;

namespace Raven.Database.Config
{
	internal static class MemoryStatistics
	{
		private static bool failedToGetAvailablePhysicalMemory;
		private static bool failedToGetTotalPhysicalMemory;
		private static int memoryLimit;

		/// <summary>
		///  This value is in MB
		/// </summary>
		public static int TotalPhysicalMemory
		{
			get
			{
				if (failedToGetTotalPhysicalMemory)
					return -1;

				if (Type.GetType("Mono.Runtime") != null)
				{
					var pc = new PerformanceCounter("Mono Memory", "Total Physical Memory");
					var totalPhysicalMemoryMegabytes = (int)(pc.RawValue / 1024 / 1024);
					if (totalPhysicalMemoryMegabytes == 0)
						totalPhysicalMemoryMegabytes = 128; // 128MB, the Mono runtime default
					return totalPhysicalMemoryMegabytes;
				}
#if __MonoCS__
				throw new PlatformNotSupportedException("This build can only run on Mono");
#else
				try
				{
					return (int) (new Microsoft.VisualBasic.Devices.ComputerInfo().TotalPhysicalMemory/1024/1024);
				}
				catch
				{
					failedToGetTotalPhysicalMemory = true;
					return -1;
				}
#endif
			}
		}

		public static bool MaxParallelismSet { get; private set; }
		private static int maxParallelism;
		public static int MaxParallelism
		{
			get { return maxParallelism; }
			set
			{
				maxParallelism = value;
				MaxParallelismSet = true;
			}
		}

		private static bool memoryLimitSet;

		/// <summary>
		/// This value is in MB
		/// </summary>
		public static int MemoryLimit
		{
			get { return memoryLimit; }
			set
			{
				memoryLimit = value;
				memoryLimitSet = true;
			}
		}

		public static int AvailableMemory
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
								if (memoryLimitSet)
									return Math.Min(MemoryLimit, Convert.ToInt32(match.Groups[1].Value)/1024);
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
						return memoryLimitSet ? Math.Min(MemoryLimit, availablePhysicalMemoryInMb) : availablePhysicalMemoryInMb;
					}

					// we are in 32 bits mode, but the _system_ may have more than 4 GB available
					// so we have to check the _address space_ as well as the available memory
					// 32bit processes are limited to 1.5GB of heap memory
					var workingSetMb = (int)(Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024);
					return memoryLimitSet ? Math.Min(MemoryLimit, Math.Min(1536 - workingSetMb, availablePhysicalMemoryInMb)) : Math.Min(1536 - workingSetMb, availablePhysicalMemoryInMb);
				}
				catch
				{
					failedToGetAvailablePhysicalMemory = true;
					return -1;
				}
#endif
			}
		}

		static readonly bool runningOnMono = Type.GetType("Mono.Runtime") != null;

		private static bool RunningOnMono
		{
			get { return runningOnMono; }
		}
	}
}