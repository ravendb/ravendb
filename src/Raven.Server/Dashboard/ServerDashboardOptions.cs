using System;

namespace Raven.Server.Dashboard
{
    public class ServerDashboardOptions
    {
        public TimeSpan MachineResourcesThrottle { get; set; } = TimeSpan.FromSeconds(1);

        public TimeSpan DatabasesInfoThrottle { get; set; } = TimeSpan.FromSeconds(3);

        public TimeSpan ThreadsInfoThrottle { get; set; } = TimeSpan.FromSeconds(1);
    }
}
