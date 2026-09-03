using System;
using System.Collections.Generic;

namespace Raven.Quill.Cdc;

internal sealed class CdcSinkPerformanceRaw
{
    public List<CdcPerfTaskRaw> Results { get; set; } = [];
}

internal sealed class CdcPerfTaskRaw
{
    public long TaskId { get; set; }
    public string? TaskName { get; set; }
    public List<CdcPerfProcessRaw> Stats { get; set; } = [];
}

internal sealed class CdcPerfProcessRaw
{
    public List<CdcPerfBatchRaw> Performance { get; set; } = [];
}

internal sealed class CdcPerfBatchRaw
{
    public int Id { get; set; }
    public DateTime Started { get; set; }
    public DateTime? Completed { get; set; }
    public double DurationInMs { get; set; }
    public int NumberOfReadMessages { get; set; }
    public int NumberOfProcessedMessages { get; set; }
    public string? BatchPullStopReason { get; set; }
    public int ScriptProcessingErrorCount { get; set; }
    public int ReadErrorCount { get; set; }
    public bool? SuccessfullyProcessed { get; set; }
}
