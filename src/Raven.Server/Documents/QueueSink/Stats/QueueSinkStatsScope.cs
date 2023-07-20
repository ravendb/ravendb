using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.QueueSink.Stats;

public class QueueSinkStatsScope : StatsScope<QueueSinkRunStats, QueueSinkStatsScope>
{
    private readonly QueueSinkRunStats _stats;

    public QueueSinkStatsScope(QueueSinkRunStats stats, bool start = true) : base(stats, start)
    {
        _stats = stats;
    }

    protected override QueueSinkStatsScope OpenNewScope(QueueSinkRunStats stats, bool start)
    {
        return new QueueSinkStatsScope(stats, start);
    }

    public QueueSinkPerformanceOperation ToQueueSinkPerformanceOperation(string name)
    {
        var operation = new QueueSinkPerformanceOperation(Duration)
        {
            Name = name
        };

        if (Scopes != null)
        {
            operation.Operations = Scopes
                .Select(x => x.Value.ToQueueSinkPerformanceOperation(x.Key))
                .ToArray();
        }

        return operation;
    }

    public void RecordConsumedMessage()
    {
        _stats.NumberOfConsumedMessages++;
    }
}
