using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.Contracts;

namespace Raven.Quill.Wizard;

internal sealed class WizardState
{
    public static string DocumentIdFor(string slug) => $"wizard-state/{slug}";

    public string? Provider { get; set; }

    public string? SourceConnectionString { get; set; }

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    public CdcSinkSourceSchema? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }

    public CdcSinkConfiguration? LastMapConfiguration { get; set; }
    public DateTime? LastMapAt { get; set; }

    /// <summary>The api's plan for this app, set when the proposal agent first answers. The api holds the plan itself.</summary>
    public string? PlanId { get; set; }

    /// <summary>The tables the operator handed the planner, so apply reports coverage against them rather than the whole discovery.</summary>
    public SelectedSourceTable[]? PlanSelectedTables { get; set; }
}
