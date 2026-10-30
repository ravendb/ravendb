using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Quill.Contracts;

namespace Raven.Quill.Wizard;

internal sealed class WizardState
{
    public static string DocumentIdFor(string slug) => $"wizard-state/{slug}";

    /// <summary>Kept for a reloaded wizard to show again; the app takes its name only when it is provisioned.</summary>
    public string? AppName { get; set; }

    public string? Provider { get; set; }

    public string? SourceConnectionString { get; set; }

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    /// <summary>The schemas the operator asked discovery to cover; empty means the connection's default one.</summary>
    public string[]? DiscoverSchemas { get; set; }

    public CdcSinkSourceSchema? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }

    /// <summary>The tables the operator selected, for a reloaded wizard to show again.</summary>
    public SelectedSourceTable[]? SelectedTables { get; set; }

    /// <summary>The wizard step the operator last moved on to, where a reloaded wizard continues.</summary>
    public string? Step { get; set; }

    public CdcSinkConfiguration? LastMapConfiguration { get; set; }
    public DateTime? LastMapAt { get; set; }

    /// <summary>
    /// The version of each registered collection LastMapConfiguration was applied from; null when it was mapped
    /// by hand. An edit reopens the plan only while it still holds these versions, so the planner never shows a
    /// plan the app does not run on.
    /// </summary>
    public Dictionary<string, int>? AppliedPlanVersions { get; set; }

    /// <summary>The api's plan for this app, set when the proposal agent first answers. The api holds the plan itself.</summary>
    public string? PlanId { get; set; }

    /// <summary>The tables the operator handed the planner, so apply reports coverage against them rather than the whole discovery.</summary>
    public SelectedSourceTable[]? PlanSelectedTables { get; set; }
}
