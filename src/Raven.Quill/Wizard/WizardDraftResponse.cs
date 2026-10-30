using Raven.Quill.Contracts;

namespace Raven.Quill.Wizard;

/// <summary>What the wizard stored for an app, so a reloaded wizard continues where it was.</summary>
public sealed record WizardDraftResponse(
    bool IsAppCreated,
    string AppName,
    string Provider,
    string ConnectionString,
    bool IsConnected,
    string[] DiscoverSchemas,
    DiscoverResponse? Discovery,
    SelectedSourceTable[] SelectedTables,
    string? Step,
    bool HasPlan,
    SelectedSourceTable[] PlanSelectedTables,
    Dictionary<string, int>? AppliedPlanVersions);
