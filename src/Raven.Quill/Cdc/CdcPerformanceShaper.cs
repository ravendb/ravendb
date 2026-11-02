using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Quill.Cdc;
using Raven.Quill.Contracts;

namespace Raven.Quill.Cdc;

internal static class CdcPerformanceShaper
{
    private static readonly TimeSpan ActiveWindow = TimeSpan.FromSeconds(60);

    internal static IEnumerable<CdcPerfBatchRaw> Batches(CdcSinkPerformanceRaw raw) =>
        (raw.Results ?? new List<CdcPerfTaskRaw>())
        .SelectMany(t => t.Stats ?? new List<CdcPerfProcessRaw>())
        .SelectMany(p => p.Performance ?? new List<CdcPerfBatchRaw>());

    public static CdcPerformanceResponse Shape(
        CdcSinkPerformanceRaw raw, bool disabled, DateTime nowUtc, DateTime? lastActivityAt,
        CdcSinkErrorsRaw? storedErrors = null)
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

        // Durable last-activity from the persisted state doc keeps lag/status meaningful after a restart
        // empties the in-memory batch window (lastSyncAt). Use whichever timestamp is most recent.
        var lastActivityUtc = lastActivityAt is { } la ? Utc(la) : (DateTime?)null;
        DateTime? effectiveActivity = lastSyncAt;
        if (lastActivityUtc is { } lau && (effectiveActivity is null || lau > effectiveActivity))
            effectiveActivity = lau;

        var errorsSince = ErrorsCutoff(batches, lastActivityUtc);
        var errorCount = Math.Max(BatchErrorsSince(batches, errorsSince), ErrorsSince(storedErrors, errorsSince).Count());

        int? lagSeconds = effectiveActivity is { } last ? Math.Max(0, (int)(nowUtc - last).TotalSeconds) : null;

        var enabled = disabled == false;
        var status =
            disabled ? "disabled" :
            errorCount > 0 ? "error" :
            inProgress || (effectiveActivity is { } l && nowUtc - l <= ActiveWindow) ? "active" :
            "idle";

        return new CdcPerformanceResponse(enabled, status, lastSyncAt, lagSeconds, recentReads, recentWrites, errorCount, points);
    }

    public static bool HasErrorsSince(CdcSinkPerformanceRaw raw, CdcSinkErrorsRaw? storedErrors, DateTime? lastActivityAt)
    {
        var batches = Batches(raw).ToArray();
        var since = ErrorsCutoff(batches, lastActivityAt is { } la ? Utc(la) : null);
        return BatchErrorsSince(batches, since) > 0 || ErrorsSince(storedErrors, since).Any();
    }

    private static DateTime? ErrorsCutoff(IEnumerable<CdcPerfBatchRaw> batches, DateTime? lastActivityUtc)
    {
        DateTime? lastCleanSyncAt = null;
        foreach (var b in batches)
        {
            if (b.Completed is { } completed && ErrorsOf(b) == 0)
            {
                var c = Utc(completed);
                if (lastCleanSyncAt is null || c > lastCleanSyncAt)
                    lastCleanSyncAt = c;
            }
        }

        return Later(lastCleanSyncAt, lastActivityUtc);
    }

    private static int BatchErrorsSince(IEnumerable<CdcPerfBatchRaw> batches, DateTime? since) =>
        batches.Where(b => IsAfter(Utc(b.Started), since)).Sum(ErrorsOf);

    private static int ErrorsOf(CdcPerfBatchRaw batch) => batch.ScriptProcessingErrorCount + batch.ReadErrorCount;

    private static DateTime? Later(DateTime? a, DateTime? b) => a is null || (b is not null && b > a) ? b : a;

    private static bool IsAfter(DateTime at, DateTime? since) => since is null || at > since;

    public static IEnumerable<CdcTaskErrorRaw> ErrorsSince(CdcSinkErrorsRaw? raw, DateTime? since) =>
        (raw?.Results ?? new List<CdcTaskErrorsRaw>())
            .SelectMany(t => (t.ProcessErrors ?? new List<CdcTaskErrorRaw>())
                .Concat(t.ItemErrors ?? new List<CdcTaskErrorRaw>()))
            .Where(e => IsAfter(Utc(e.CreatedAt), since));

    // Default cap on returned error details — mirrors the rolling perf window (~25 entries).
    private const int MaxErrors = 25;

    public static CdcError[] ShapeErrors(CdcSinkErrorsRaw raw) =>
        (raw.Results ?? new List<CdcTaskErrorsRaw>())
            .SelectMany(t => (t.ProcessErrors ?? new List<CdcTaskErrorRaw>())
                .Concat(t.ItemErrors ?? new List<CdcTaskErrorRaw>()))
            .Select(e => new CdcError(
                e.TaskName,
                Utc(e.CreatedAt),
                e.Step,
                e.Error,
                e.DocumentId,
                e.AffectedDocumentsCount))
            .OrderByDescending(e => e.CreatedAt)
            .Take(MaxErrors)
            .ToArray();

    // Mark an outbound timestamp UTC so it serializes with the Z designator (the perf feed's
    // DateTimes may arrive Unspecified). Mirrors MetricsReadService.Utc.
    private static DateTime Utc(DateTime d) => d.Kind switch
    {
        DateTimeKind.Utc => d,
        DateTimeKind.Local => d.ToUniversalTime(),
        _ => DateTime.SpecifyKind(d, DateTimeKind.Utc),
    };
}
