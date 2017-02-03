using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public class WinLowMemoryNotification : AbstractLowMemoryNotification
    {
        private static Logger _logger;

        private const int LowMemoryResourceNotification = 0;

        const uint WAIT_FAILED = 0xFFFFFFFF;
        const uint WAIT_TIMEOUT = 0x00000102;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateMemoryResourceNotification(int notificationType);


        [DllImport("kernel32.dll")]
        private static extern uint WaitForSingleObject(IntPtr hHandle, uint dwMilliseconds);


        [DllImport("kernel32.dll")]
        private static extern uint WaitForMultipleObjects(uint nCount, IntPtr[] lpHandles, bool bWaitAll, uint dwMilliseconds);

        [DllImport("Kernel32.dll", SetLastError = true, CallingConvention = CallingConvention.Winapi, CharSet = CharSet.Unicode)]
        private static extern IntPtr CreateEvent(IntPtr lpEventAttributes, bool bManualReset, bool bInitialState, string lpName);

        [DllImport("Kernel32.dll", SetLastError = true)]
        public static extern bool SetEvent(IntPtr hEvent);

        [DllImport("Kernel32.dll")]
        public static extern IntPtr GetCurrentProcess();

        private readonly IntPtr lowMemorySimulationEvent;
        private readonly IntPtr lowMemoryNotificationHandle;

        public WinLowMemoryNotification(CancellationToken shutdownNotification, RavenConfiguration configuration)
        {
            _logger = LoggingSource.Instance.GetLogger<WinLowMemoryNotification>(configuration.ResourceName);
            lowMemorySimulationEvent = CreateEvent(IntPtr.Zero, false, false, null);
            lowMemoryNotificationHandle = CreateMemoryResourceNotification(LowMemoryResourceNotification); // the handle will be closed by the system if the process terminates

            var appDomainUnloadEvent = CreateEvent(IntPtr.Zero, false, false, null);
            shutdownNotification.Register(() => SetEvent(appDomainUnloadEvent));

            if (lowMemoryNotificationHandle == null)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("lowMemoryNotificationHandle is null. might be because of permissions issue.");
                throw new Win32Exception();
            }

            new Thread(MonitorMemoryUsage)
            {
                IsBackground = true,
                Name = "Low memory notification thread"
            }.Start(appDomainUnloadEvent);
        }

        private void MonitorMemoryUsage(object state)
        {
            IntPtr appDomainUnloadEvent = (IntPtr) state;
            while (true)
            {
                var waitForResult = WaitForMultipleObjects(3,
                    new[] {lowMemoryNotificationHandle, appDomainUnloadEvent, lowMemorySimulationEvent},
                    false,
                    5*60*1000);

                switch (waitForResult)
                {
                    case 0: // lowMemoryNotificationHandle
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Low memory detected, will try to reduce memory usage...");
                        RunLowMemoryHandlers();
                        break;
                    case 1:
                        // app domain unload
                        return;
                    case 2: // LowMemorySimulationEvent
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Low memory simulation, will try to reduce memory usage...");
                        RunLowMemoryHandlers();
                        break;
                    case 3: // SoftMemoryReleaseEvent
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Releasing memory before Garbage Collection operation");
                        RunLowMemoryHandlers();
                        break;
                    case WAIT_TIMEOUT:
                        ClearInactiveHandlers();
                        break;
                    case WAIT_FAILED:
                        if (_logger.IsInfoEnabled)
                            _logger.Info(
                                "Failure when trying to wait for low memory notification. No low memory changes will be raised.");
                        break;
                }
                // prevent triggering the event too frequent when the low memory notification object is in the signaled state
                WaitForSingleObject(appDomainUnloadEvent, 60 * 1000);
            }
        }

        public override void SimulateLowMemoryNotification()
        {
            SetEvent(lowMemorySimulationEvent);
        }
    }
}