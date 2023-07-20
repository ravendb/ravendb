using System;

namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class QueueSinkPerformanceOperation
{
    public QueueSinkPerformanceOperation(TimeSpan duration)
    {
        DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        Operations = Array.Empty<QueueSinkPerformanceOperation>();
    }

    public string Name { get; set; }

    public double DurationInMs { get; }

    public QueueSinkPerformanceOperation[] Operations { get; set; }
}
