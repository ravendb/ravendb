using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Quill.Contracts;

namespace Raven.Quill.Cdc;

/// <summary>
/// Folds the rolling per-batch CDC perf stats into the <see cref="CdcPerformanceResponse"/>
/// snapshot. Pure (no I/O) so it can be unit-tested without a live CDC source.
/// </summary>
internal static class CdcPerformanceShaper
{
    // A batch completing within this window counts the sink as actively syncing.
    private static readonly TimeSpan ActiveWindow = TimeSpan.FromSeconds(60);

    internal static IEnumerable<CdcPerfBatchRaw> Batches(CdcSinkPerformanceRaw raw) =>
        (raw.Results ?? new List<CdcPerfTaskRaw>())
            .SelectMany(t => t.Stats ?? new List<CdcPerfProcessRaw>())
            .SelectMany(p => p.Performance ?? new List<CdcPerfBatchRaw>());

    public static CdcPerformanceResponse Shape(CdcSinkPerformanceRaw raw, bool configured, bool disabled, DateTime nowUtc)
    {
        var batches = Batches(raw)
            .OrderBy(b => b.Started)
            .ToArray();

        var points = batches
            .Select(b => new CdcBatchPoint(
                Utc(b.Started),
                b.Completed is { } c ? Utc(c) : null,
                b.DurationInMs,
                b.NumberOfReadMessages,
                b.NumberOfProcessedMessages,
                b.ScriptProcessingErrorCount + b.ReadErrorCount,
                b.BatchPullStopReason))
            .ToArray();

        long recentReads = batches.Sum(b => (long)b.NumberOfReadMessages);
        long recentWrites = batches.Sum(b => (long)b.NumberOfProcessedMessages);
        int errorCount = batches.Sum(b => b.ScriptProcessingErrorCount + b.ReadErrorCount);

        DateTime? lastSyncAt = null;
        var inProgress = false;
        foreach (var b in batches)
        {
            if (b.Completed is { } completed)
            {
                var c = Utc(completed);
                if (lastSyncAt is null || c > lastSyncAt)
                    lastSyncAt = c;
            }
            else
            {
                inProgress = true;
            }
        }

        int? lagSeconds = lastSyncAt is { } last ? Math.Max(0, (int)(nowUtc - last).TotalSeconds) : null;

        var enabled = configured && disabled == false;
        var status =
            configured == false ? "not-configured" :
            disabled ? "disabled" :
            errorCount > 0 ? "error" :
            inProgress || (lastSyncAt is { } l && nowUtc - l <= ActiveWindow) ? "active" :
            "idle";

        return new CdcPerformanceResponse(enabled, status, lastSyncAt, lagSeconds, recentReads, recentWrites, errorCount, points);
    }

    // Mark an outbound timestamp UTC so it serializes with the Z designator (the perf feed's
    // DateTimes may arrive Unspecified). Mirrors MetricsReadService.Utc.
    private static DateTime Utc(DateTime d) => d.Kind switch
    {
        DateTimeKind.Utc => d,
        DateTimeKind.Local => d.ToUniversalTime(),
        _ => DateTime.SpecifyKind(d, DateTimeKind.Utc),
    };
}
