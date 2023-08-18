using System;
using System.Diagnostics;
using Raven.Server.Documents.QueueSink.Stats.Performance;
using Raven.Server.Utils.Stats;
using Sparrow;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.QueueSink.Stats;

public class QueueSinkStatsAggregator : StatsAggregator<QueueSinkRunStats, QueueSinkStatsScope>
{
    private readonly IStatsAggregator _lastStats;
    private volatile QueueSinkPerformanceStats _performanceStats;

    public QueueSinkStatsAggregator(int id, IStatsAggregator lastStats) : base(id, lastStats)
    {
        _lastStats = lastStats;
    }

    public void Start()
    {
        SetStartTime(_lastStats);
    }

    public override QueueSinkStatsScope CreateScope()
    {
        Debug.Assert(Scope == null);

        return Scope = new QueueSinkStatsScope(Stats, start: false);
    }

    public QueueSinkPerformanceStats ToPerformanceStats()
    {
        if (_performanceStats != null)
            return _performanceStats;

        lock (Stats)
        {
            if (_performanceStats != null)
                return _performanceStats;

            return _performanceStats = CreatePerformanceStats(completed: true);
        }
    }

    public QueueSinkPerformanceStats ToPerformanceLiveStatsWithDetails()
    {
        if (_performanceStats != null)
            return _performanceStats;

        if (Scope == null || Stats == null)
            return null;

        if (Completed)
            return ToPerformanceStats();

        return CreatePerformanceStats(completed: false);
    }

    public QueueSinkPerformanceStats ToPerformanceLiveStats()
    {
        throw new System.NotImplementedException();
    }

    private QueueSinkPerformanceStats CreatePerformanceStats(bool completed)
    {
        return new QueueSinkPerformanceStats(Scope.Duration)
        {
            Id = Id,
            Started = StartTime,
            Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
            Details = Scope.ToQueueSinkPerformanceOperation("Consume"),
            NumberOfReadMessages = Stats.NumberOfReadMessages,
            NumberOfProcessedMessages = Stats.NumberOfProcessedMessages,
            ReadErrorCount = Stats.ReadErrorCount,
            ScriptProcessingErrorCount = Stats.ScriptProcessingErrorCount,
            BatchPullStopReason = Stats.BatchPullStopReason,
            CurrentlyAllocated = new Size(Stats.CurrentlyAllocated.GetValue(SizeUnit.Bytes)),
            SuccessfullyProcessed = Stats.ReadErrorCount == 0 && Stats.ScriptProcessingErrorCount == 0,
        };
    }
}
