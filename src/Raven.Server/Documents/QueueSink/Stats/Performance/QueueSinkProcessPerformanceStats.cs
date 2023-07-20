namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class QueueSinkProcessPerformanceStats
{
    public string TransformationName { get; set; }
    public QueueSinkPerformanceStats[] Performance { get; set; }
}
