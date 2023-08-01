using System;

namespace Raven.Server.Dashboard;

public sealed class ThreadsInfoOptions
{
    public TimeSpan ThreadsInfoThrottle { get; set; } = TimeSpan.FromSeconds(1);
}
