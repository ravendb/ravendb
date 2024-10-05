using System;
using System.Diagnostics;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Logging;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.NotificationCenter;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Logging;
using Sparrow.Utils;

namespace Raven.Server.Utils.Cpu
{
    public static class CpuHelper
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer(typeof(CpuHelper));

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
            ServerNotificationCenter notificationCenter)
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
#pragma warning disable CA1416 // Validate platform compatibility
                return Bits.NumberOfSetBits(process.ProcessorAffinity.ToInt64());
#pragma warning restore CA1416 // Validate platform compatibility
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
                return (0, 0);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failure to get process times, error: {e.Message}", e);

                return (0, 0);
            }
        }
    }
}
