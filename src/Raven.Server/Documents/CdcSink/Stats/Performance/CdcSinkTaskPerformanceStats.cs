namespace Raven.Server.Documents.CdcSink.Stats.Performance;

public class CdcSinkTaskPerformanceStats
{
    public long TaskId { get; set; }

    public string TaskName { get; set; }

    public CdcSinkProcessPerformanceStats[] Stats { get; set; }
}
