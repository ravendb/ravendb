using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Database.Util;

namespace Raven.Database.Config
{
    internal interface ILowMemoryHandler
    {
        void HandleLowMemory();
    }

    internal static class MemoryStatistics
    {
        private static ILog log = LogManager.GetCurrentClassLogger();

        private const int LowMemoryResourceNotification = 0;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateMemoryResourceNotification(int notificationType);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool QueryMemoryResourceNotification(IntPtr hResNotification, out bool isResourceStateMet);

        [DllImport("kernel32.dll")]
        private static extern uint WaitForMultipleObjects(uint nCount, IntPtr[] lpHandles, bool bWaitAll, uint dwMilliseconds);

        [DllImport("Kernel32.dll", SetLastError = true, CallingConvention = CallingConvention.Winapi, CharSet = CharSet.Auto)]
        private static extern IntPtr CreateEvent(IntPtr lpEventAttributes, bool bManualReset, bool bIntialState, string lpName);

        [DllImport("Kernel32.dll", SetLastError = true)]
        public static extern bool SetEvent(IntPtr hEvent);

        private static bool failedToGetAvailablePhysicalMemory;
        private static bool failedToGetTotalPhysicalMemory;
        private static int memoryLimit;
        private static readonly IntPtr lowMemoryNotificationHandle;
        private static readonly ConcurrentSet<WeakReference<ILowMemoryHandler>> LowMemoryHandlers = new ConcurrentSet<WeakReference<ILowMemoryHandler>>();
        private static readonly IntPtr LowMemorySimulationEvent = CreateEvent(IntPtr.Zero, false, false, null);

        static MemoryStatistics()
        {
            lowMemoryNotificationHandle = CreateMemoryResourceNotification(LowMemoryResourceNotification); // the handle will be closed by the system if the process terminates

            var appDomainUnloadEvent = CreateEvent(IntPtr.Zero, true, false, null);

            AppDomain.CurrentDomain.DomainUnload += (sender, args) => SetEvent(appDomainUnloadEvent);

            if (lowMemoryNotificationHandle == null)
                throw new Win32Exception();

            new Thread(() =>
            {
                const UInt32 WAIT_FAILED = 0xFFFFFFFF;
                const UInt32 WAIT_TIMEOUT = 0x00000102;
                while (true)
                {
                    var waitForResult = WaitForMultipleObjects(3,
						new[] { lowMemoryNotificationHandle, appDomainUnloadEvent, LowMemorySimulationEvent }, false, 
						5 * 60 * 1000);

				handleWaitResults:
                    switch (waitForResult)
                    {
                        case 0: // lowMemoryNotificationHandle
                            log.Warn("Low memory detected, will try to reduce memory usage...");

                            RunLowMemoryHandlers();
							// prevent triggering the event too frequent when the low memory notification object 
							// is in the signaled state
							waitForResult = WaitForMultipleObjects(2,
			                    new[] {appDomainUnloadEvent, LowMemorySimulationEvent}, false,
			                    60*1000);
		                    goto handleWaitResults;
                        case 1:
                            // app domain unload
                            return;
                        case 2: // LowMemorySimulationEvent
                            log.Warn("Low memory simulation, will try to reduce memory usage...");

                            RunLowMemoryHandlers();
                            break;
                        case WAIT_TIMEOUT:
                            ClearInactiveHandlers();
                            break;
                        case WAIT_FAILED:
                            log.Warn("Failure when trying to wait for low memory notification. No low memory notifications will be raised.");
                            break;
                    }
                }
            })
            {
                IsBackground = true,
                Name = "Low memory notification thread"
            }.Start();
        }

        public static void SimulateLowMemoryNotification()
        {
            SetEvent(LowMemorySimulationEvent);
        }

        private static void RunLowMemoryHandlers()
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in LowMemoryHandlers)
            {
                ILowMemoryHandler handler;
                if (lowMemoryHandler.TryGetTarget(out handler))
                {
                    try
                    {
                        handler.HandleLowMemory();
                    }
                    catch (Exception e)
                    {
                        log.Error("Failure to process low memory notification (low memory handler - " + handler + ")", e);
                    }
                }
                else
                    inactiveHandlers.Add(lowMemoryHandler);
            }

            inactiveHandlers.ForEach(x => LowMemoryHandlers.TryRemove(x));
        }

        private static void ClearInactiveHandlers()
        {
            var inactiveHandlers = new List<WeakReference<ILowMemoryHandler>>();

            foreach (var lowMemoryHandler in LowMemoryHandlers)
            {
                ILowMemoryHandler handler;
                if (lowMemoryHandler.TryGetTarget(out handler) == false)
                    inactiveHandlers.Add(lowMemoryHandler);
            }

            inactiveHandlers.ForEach(x => LowMemoryHandlers.TryRemove(x));
        }

        public static bool IsLowMemory
        {
            get
            {
                bool isResourceStateMet;
                bool succeeded = QueryMemoryResourceNotification(lowMemoryNotificationHandle, out isResourceStateMet);

                if (!succeeded)
                {
                    throw new InvalidOperationException("Call to QueryMemoryResourceNotification failed!");
                }

                return isResourceStateMet;
            }
        }

        public static void RegisterLowMemoryHandler(ILowMemoryHandler handler)
        {
            LowMemoryHandlers.Add(new WeakReference<ILowMemoryHandler>(handler));
        }

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
                    return (int)(new Microsoft.VisualBasic.Devices.ComputerInfo().TotalPhysicalMemory / 1024 / 1024);
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
            get
            {
                if (MaxParallelismSet == false)
                {
                    return (Environment.ProcessorCount * 2);
                }
                return maxParallelism;
            }
            set
            {
                if (value == 0)
                    throw new ArgumentException("You cannot set the max parallelism to zero");

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
                                    return Math.Min(MemoryLimit, Convert.ToInt32(match.Groups[1].Value) / 1024);
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