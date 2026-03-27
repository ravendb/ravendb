namespace Raven.Server.Documents.CdcSink.Stats.Performance;

public class CdcSinkProcessPerformanceStats
{
    public string ScriptName { get; set; }
    public CdcSinkPerformanceStats[] Performance { get; set; }
}
