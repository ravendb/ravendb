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

        public PosixLowMemoryNotification(CancellationToken shutdownNotification, RavenConfiguration configuration)
        {
            _shutdownNotification = shutdownNotification;
            _configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<PosixLowMemoryNotification>(configuration.DatabaseName);
            new Thread(MonitorMemoryUsage)
            {
                IsBackground = true,
                Name = "Low memory notification thread"
            }.Start();
        }

        private void MonitorMemoryUsage()
        {
            int clearInactiveHandlersCounter = 0;

            while (true)
            {
                SpinWait.SpinUntil(() => false, TimeSpan.FromSeconds(5));
                if (_shutdownNotification.IsCancellationRequested)
                    return;

                if (++clearInactiveHandlersCounter > 60) // 5 minutes == WaitAny 5 Secs * 60
                {
                    clearInactiveHandlersCounter = 0;
                    ClearInactiveHandlers();
                    continue;
                }

                var availableMem = MemoryInformation.GetMemoryInfo(_configuration).AvailableMemory;
                if (availableMem < _configuration.Memory.LowMemoryForLinuxDetection)
                {
                    clearInactiveHandlersCounter = 0;
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Low memory detected, will try to reduce memory usage...");
                    RunLowMemoryHandlers();
                    Thread.Sleep(TimeSpan.FromSeconds(60)); // prevent triggering the event to frequent when the low memory notification object is in the signaled state
                }
            }
        }

        public override void SimulateLowMemoryNotification()
        {
            
        }
    }
}