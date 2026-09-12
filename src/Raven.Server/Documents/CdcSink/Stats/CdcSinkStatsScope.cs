using System.Linq;
using Raven.Server.Documents.CdcSink.Stats.Performance;
using Raven.Server.Utils.Stats;
using Sparrow;

namespace Raven.Server.Documents.CdcSink.Stats;

public class CdcSinkStatsScope : StatsScope<CdcSinkRunStats, CdcSinkStatsScope>
{
    private readonly CdcSinkRunStats _stats;

    public CdcSinkStatsScope(CdcSinkRunStats stats, bool start = true) : base(stats, start)
    {
        _stats = stats;
    }

    protected override CdcSinkStatsScope OpenNewScope(CdcSinkRunStats stats, bool start)
    {
        return new CdcSinkStatsScope(stats, start);
    }

    public CdcSinkPerformanceOperation ToCdcSinkPerformanceOperation(string name)
    {
        var operation = new CdcSinkPerformanceOperation(Duration)
        {
            Name = name
        };

        if (Scopes != null)
        {
            operation.Operations = Scopes
                .Select(x => x.Value.ToCdcSinkPerformanceOperation(x.Key))
                .ToArray();
        }

        return operation;
    }

    public void RecordReadMessage()
    {
        _stats.NumberOfReadMessages++;
    }

    public void RecordProcessedMessage()
    {
        _stats.NumberOfProcessedMessages++;
    }

    public void RecordReadError()
    {
        _stats.ReadErrorCount++;
    }

    public void RecordScriptProcessingError()
    {
        _stats.ScriptProcessingErrorCount++;
    }

    public void RecordPullCompleteReason(string reason)
    {
        _stats.BatchPullStopReason = reason;
    }

    public void RecordCurrentlyAllocated(long allocatedInBytes)
    {
        _stats.CurrentlyAllocated = new Size(allocatedInBytes, SizeUnit.Bytes);
    }
}
