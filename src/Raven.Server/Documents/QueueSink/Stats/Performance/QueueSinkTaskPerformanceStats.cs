using Raven.Client.Documents.Operations.ETL.Queue;

namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class QueueSinkTaskPerformanceStats
{
    public long TaskId { get; set; }

    public string TaskName { get; set; }

    public QueueBrokerType BrokerType { get; set; }

    public QueueSinkProcessPerformanceStats[] Stats { get; set; }
}
