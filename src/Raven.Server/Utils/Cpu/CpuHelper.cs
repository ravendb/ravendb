using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Dashboard;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Utils.Cpu
{
    public static class CpuHelper
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MachineResources>("Server");

        internal static ICpuUsageCalculator GetOSCpuUsageCalculator()
        {
            ICpuUsageCalculator calculator;
            if (PlatformDetails.RunningOnPosix == false)
            {
                calculator = new WindowsCpuUsageCalculator();
            }
            else if (PlatformDetails.RunningOnMacOsx)
            {
                calculator = new MacInfoCpuUsageCalculator();
            }
            else
            {
                calculator = new LinuxCpuUsageCalculator();
            }
            calculator.Init();
            return calculator;
        }

        internal static ExtensionPointCpuUsageCalculator GetExtensionPointCpuUsageCalculator(
            JsonContextPool contextPool,
            MonitoringConfiguration configuration,
            NotificationCenter.NotificationCenter notificationCenter)
        {
            var extensionPoint = new ExtensionPointCpuUsageCalculator(
                contextPool,
                configuration.CpuUsageMonitorExec,
                configuration.CpuUsageMonitorExecArguments,
                notificationCenter);

            

            return extensionPoint;
        }

        public static long GetNumberOfActiveCores(Process process)
        {
            try
            {
                return Bits.NumberOfSetBits(process.ProcessorAffinity.ToInt64());
            }
            catch (NotSupportedException)
            {
                return ProcessorInfo.ProcessorCount;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure to get the number of active cores", e);

                return ProcessorInfo.ProcessorCount;
            }
        }

        public static (long TotalProcessorTimeTicks, long TimeTicks) GetProcessTimes(Process process)
        {
            try
            {
                var timeTicks = SystemTime.UtcNow.Ticks;
                var totalProcessorTime = process.TotalProcessorTime.Ticks;
                return (TotalProcessorTimeTicks: totalProcessorTime, TimeTicks: timeTicks);
            }
            catch (NotSupportedException)
            {
                return TryGetProcessTimesForLinux();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failure to get process times, error: {e.Message}", e);

                return (0, 0);
            }
        }

        private static (long TotalProcessorTimeTicks, long TimeTicks) TryGetProcessTimesForLinux()
        {
            if (PlatformDetails.RunningOnLinux == false)
                return (0, 0);

            try
            {
                long timeTicks;
                long tmsStime;
                long tmsUtime;

                if (PlatformDetails.Is32Bits == false)
                {
                    var timeSample = new TimeSample();
                    timeTicks = Syscall.times(ref timeSample);
                    tmsStime = timeSample.tms_stime;
                    tmsUtime = timeSample.tms_utime;
                }
                else
                {
                    var timeSample = new TimeSample_32bit();
                    timeTicks = Syscall.times(ref timeSample);
                    tmsStime = timeSample.tms_stime;
                    tmsUtime = timeSample.tms_utime;
                }

                if (timeTicks == -1)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Got overflow time using the times system call " + Marshal.GetLastWin32Error());

                    return (0, 0);
                }

                return (TotalProcessorTimeTicks: tmsUtime + tmsStime, TimeTicks: timeTicks);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failure to get process times for linux, error: {e.Message}", e);

                return (0, 0);
            }
        }
    }
}
