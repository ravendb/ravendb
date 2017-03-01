using System;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenterOptions
    {
        public bool PreventFromRunningBackgroundWorkers { get; set; } = false;

        public TimeSpan DatabaseStatsThrottle { get; set; } = TimeSpan.FromSeconds(5);
    }
}