using System;
using System.Diagnostics;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Utils.Stats;
using Sparrow;
using Size = Raven.Client.Util.Size;

namespace Raven.Server.Documents.CdcSink.Stats;

public class CdcSinkStatsAggregator : StatsAggregator<CdcSinkRunStats, CdcSinkStatsScope>
{
    private readonly IStatsAggregator _lastStats;
    private volatile CdcSinkPerformanceStats _performanceStats;

    public CdcSinkStatsAggregator(int id, IStatsAggregator lastStats) : base(id, lastStats)
    {
        _lastStats = lastStats;
    }

    public void Start()
    {
        SetStartTime(_lastStats);
    }

    public override CdcSinkStatsScope CreateScope()
    {
        Debug.Assert(Scope == null);

        return Scope = new CdcSinkStatsScope(Stats, start: false);
    }

    public CdcSinkPerformanceStats ToPerformanceStats()
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

    public CdcSinkPerformanceStats ToPerformanceLiveStatsWithDetails()
    {
        if (_performanceStats != null)
            return _performanceStats;

        if (Scope == null || Stats == null)
            return null;

        if (Completed)
            return ToPerformanceStats();

        return CreatePerformanceStats(completed: false);
    }

    private CdcSinkPerformanceStats CreatePerformanceStats(bool completed)
    {
        return new CdcSinkPerformanceStats(Scope.Duration)
        {
            Id = Id,
            Started = StartTime,
            Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
            Details = Scope.ToCdcSinkPerformanceOperation("Consume"),
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
