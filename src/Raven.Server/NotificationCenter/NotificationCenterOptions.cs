using System;

namespace Raven.Server.NotificationCenter
{
    public sealed class NotificationCenterOptions
    {
        public TimeSpan DatabaseStatsThrottle { get; set; } = TimeSpan.FromSeconds(5);
    }
}