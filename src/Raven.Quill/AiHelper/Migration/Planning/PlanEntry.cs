using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Planning;

public enum PlanEntryStatus
{
    Registered,
    Rejected,
    Removed
}

public sealed class PlanEntry
{
    public string? Area { get; set; }

    public string? Rationale { get; set; }

    public CdcSinkTableConfig? Config { get; set; }

    public int Version { get; set; }

    public string[] Warnings { get; set; } = [];

    public PlanEntryStatus Status { get; set; }
}
