using System;
using Raven.Client.Util;

namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class QueueSinkPerformanceStats
{
    public QueueSinkPerformanceStats(TimeSpan duration)
    {
        DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
    }

    public int Id { get; set; }

    public DateTime Started { get; set; }

    public DateTime? Completed { get; set; }

    public double DurationInMs { get; }

    public QueueSinkPerformanceOperation Details { get; set; }

    public int NumberOfReadMessages{ get; set; }

    public int NumberOfProcessedMessages { get; set; }

    public Size CurrentlyAllocated { get; set; }
    
    public string BatchPullStopReason { get; set; }

    public int ScriptProcessingErrorCount { get; set; }

    public int ReadErrorCount { get; set; }

    public bool? SuccessfullyProcessed { get; set; }
}
