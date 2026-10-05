using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.AiHelper.Migration.Planning;

public sealed class PlanEntry
{
    public string Collection { get; set; } = string.Empty;

    public string? Rationale { get; set; }

    public CdcSinkTableConfig? Config { get; set; }

    /// <summary>1 on first registration, incremented every time the model re-emits the collection.</summary>
    public int Version { get; set; }
}
