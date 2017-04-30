using System;
using System.Threading;
using Raven.Server.Config;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public class PosixLowMemoryNotification : AbstractLowMemoryNotification
    {
        private readonly RavenConfiguration _configuration;
        private readonly ManualResetEvent _simulatedLowMemory = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);

        public PosixLowMemoryNotification(CancellationToken shutdownNotification, RavenConfiguration configuration) : base(configuration.ResourceName)
        {
            shutdownNotification.Register(() => _shutdownRequested.Set());
            _configuration = configuration;
            new Thread(MonitorMemoryUsage)
            {
                IsBackground = true,
                Name = "Low memory notification thread"
            }.Start();
        }

        private void MonitorMemoryUsage()
        {
            int clearInactiveHandlersCounter = 0;
            var handles = new WaitHandle[] { _simulatedLowMemory, _shutdownRequested };
            while (true)
            {
                switch (WaitHandle.WaitAny(handles, 5 *  1000))
                {
                    case WaitHandle.WaitTimeout:
                        if (++clearInactiveHandlersCounter > 60) // 5 minutes == WaitAny 5 Secs * 60
                        {
                            clearInactiveHandlersCounter = 0;
                            ClearInactiveHandlers();
                        }
                        var availableMem = MemoryInformation.GetMemoryInfo().AvailableMemory;
                        if (availableMem < _configuration.Memory.LowMemoryForLinuxDetection)
                        {
                            clearInactiveHandlersCounter = 0;
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Low memory detected, will try to reduce memory usage...");
                            RunLowMemoryHandlers();
                        }
                        break;
                    case 0:
                        _simulatedLowMemory.Reset();
                        RunLowMemoryHandlers();
                        break;
                    default:
                        return;
                }
            }
        }

        public override void SimulateLowMemoryNotification()
        {
            _simulatedLowMemory.Set();
        }
    }
}