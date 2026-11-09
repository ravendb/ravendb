using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.Contracts;

namespace Raven.Quill.Wizard;

internal sealed class WizardState
{
    public static string DocumentIdFor(string slug) => $"wizard-state/{slug}";

    public string? AppName { get; set; }

    public string? Provider { get; set; }

    public string? SourceConnectionString { get; set; }

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    public string[]? DiscoverSchemas { get; set; }

    public CdcSinkSourceSchema? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }

    public SelectedSourceTable[]? SelectedTables { get; set; }

    public string? Step { get; set; }

    public CdcSinkConfiguration? LastMapConfiguration { get; set; }
    public DateTime? LastMapAt { get; set; }

    public Dictionary<string, int>? AppliedPlanVersions { get; set; }

    public string? PlanId { get; set; }

    public SelectedSourceTable[]? PlanSelectedTables { get; set; }
}
