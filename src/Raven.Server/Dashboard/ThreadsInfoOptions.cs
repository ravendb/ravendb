using System;

namespace Raven.Server.Dashboard;

public class ThreadsInfoOptions
{
    public TimeSpan ThreadsInfoThrottle { get; set; } = TimeSpan.FromSeconds(5);
}
