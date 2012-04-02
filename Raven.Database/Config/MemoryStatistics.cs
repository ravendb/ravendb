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

		public static int AvailableMemory
		{
			get
			{
				if (failedToGetAvailablePhysicalMemory)
					return -1;

				if (Type.GetType("Mono.Runtime") != null)
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
						return availablePhysicalMemoryInMb;

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
	}
}