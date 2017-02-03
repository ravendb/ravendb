using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Configuration;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Voron.Platform.Posix;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public class PosixLowMemoryNotification : AbstractLowMemoryNotification
    {
        private readonly CancellationToken _shutdownNotification;
        private readonly RavenConfiguration _configuration;
        private static Logger _logger;
        private readonly ManualResetEvent _simulatedLowMemory = new ManualResetEvent(false);
        private readonly ManualResetEvent _shutdownRequested = new ManualResetEvent(false);

        public PosixLowMemoryNotification(CancellationToken shutdownNotification, RavenConfiguration configuration)
        {
            shutdownNotification.Register(() => _shutdownRequested.Set());
            _shutdownNotification = shutdownNotification;
            _configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<PosixLowMemoryNotification>(configuration.ResourceName);
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
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Low memory detected, will try to reduce memory usage...");
                            RunLowMemoryHandlers();
                            Thread.Sleep(TimeSpan.FromSeconds(60)); // prevent triggering the event to frequent when the low memory notification object is in the signaled state
                        }
                        break;
                    case 0:
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