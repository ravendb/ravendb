namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class QueueSinkProcessPerformanceStats
{
    public string ScriptName { get; set; }
    public QueueSinkPerformanceStats[] Performance { get; set; }
}
