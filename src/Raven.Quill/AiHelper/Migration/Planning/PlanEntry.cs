using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Planning;

public enum PlanEntryStatus
{
    Registered,
    Rejected,
    Removed
}

/// <summary>
/// One collection of the plan as the api stores it; its name is the key of the entry and <c>Config.CollectionName</c>.
/// Only <see cref="PlanEntryStatus.Registered"/> entries are applied.
/// </summary>
public sealed class PlanEntry
{
    public string? Area { get; set; }

    public string? Rationale { get; set; }

    public CdcSinkTableConfig? Config { get; set; }

    /// <summary>1 on first registration, incremented every time the collection is re-registered or edited.</summary>
    public int Version { get; set; }

    public string[] Warnings { get; set; } = [];

    public PlanEntryStatus Status { get; set; }
}
