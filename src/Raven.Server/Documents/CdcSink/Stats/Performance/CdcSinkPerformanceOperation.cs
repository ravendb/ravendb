using System;

namespace Raven.Server.Documents.CdcSink.Stats.Performance;

public class CdcSinkPerformanceOperation
{
    public CdcSinkPerformanceOperation(TimeSpan duration)
    {
        DurationInMs = Math.Round(duration.TotalMilliseconds, 2);
        Operations = Array.Empty<CdcSinkPerformanceOperation>();
    }

    public string Name { get; set; }

    public double DurationInMs { get; }

    public CdcSinkPerformanceOperation[] Operations { get; set; }
}
