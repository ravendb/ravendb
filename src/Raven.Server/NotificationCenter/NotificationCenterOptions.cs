using System;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenterOptions
    {
        public TimeSpan DatabaseStatsThrottle { get; set; } = TimeSpan.FromSeconds(5);
    }
}