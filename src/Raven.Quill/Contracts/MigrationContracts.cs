using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.Contracts;

/// <summary>
/// Begins a migration: the proposal agent groups the schema the wizard discovered into areas and
/// collections. The api creates the plan and Quill remembers its id for the app.
/// </summary>
public sealed record MigrationProposalStartRequest(
    string Slug,
    SelectedSourceTable[]? SelectedTables = null,
    string? Prompt = null);

/// <summary>One more turn with the proposal agent: a question, a re-proposal, or the naming conventions.</summary>
public sealed record MigrationProposalAskRequest(string Slug, string Prompt);

/// <summary>
/// Opens an area for the first time: the area agent builds every collection the proposal gave it.
/// Called once per area; later opens read the cards from plan/get.
/// </summary>
public sealed record MigrationAreaStartRequest(string Slug, string Area);

/// <summary>One more turn with an area's agent. The message names the collections it is about.</summary>
public sealed record MigrationAreaAskRequest(string Slug, string Area, string Prompt);

public enum PlannerAgent
{
    Proposal,
    Area
}

/// <summary>
/// Stops the running turn of one agent: the proposal's, or the area's named by <paramref name="Area"/>.
/// Leaving the page does not stop a turn; only this does.
/// </summary>
public sealed record MigrationStopRequest(string Slug, [property: JsonRequired] PlannerAgent Agent, string? Area = null);

public sealed record MigrationPlanGetRequest(string Slug);

/// <summary>The plan as the api stores it and the proposal it accepted; the areas are the proposal's.</summary>
public sealed record MigrationPlanGetResponse(MigrationPlanDocument Plan, MigrationProposal? Proposal);

public sealed class MigrationPlanDocument
{
    public string Id { get; set; } = string.Empty;

    public string? LicenseId { get; set; }

    public string? CustomerId { get; set; }

    public string? Slug { get; set; }

    public NamingConventions Conventions { get; set; } = new();

    /// <summary>Keyed by collection name, every status included.</summary>
    public Dictionary<string, PlanEntry> Entries { get; set; } = new();
}

/// <summary>
/// The unsaved changes of an area view, saved together or not at all. Refused with 409 while a planner
/// turn of the app runs.
/// </summary>
public sealed record MigrationPlanSaveRequest(string Slug, MigrationPlanChange[] Changes);

public enum PlanChangeKind
{
    Edit,
    Rename,
    Move,
    Delete
}

/// <summary>
/// One change, named by <paramref name="Kind"/>: an edit carries <paramref name="Config"/> (an add when the
/// collection is new), a rename <paramref name="NewName"/>, a move <paramref name="MoveToArea"/>, and a delete
/// none of them. The api refuses a change that carries anything else. The area and the move target must be
/// areas of the accepted proposal.
/// </summary>
public sealed record MigrationPlanChange(
    PlanChangeKind Kind,
    string Area,
    string Collection,
    CdcSinkTableConfig? Config = null,
    string? NewName = null,
    string? MoveToArea = null);

/// <summary>
/// The api's answer to a save, on 200 and 422 alike. <paramref name="Issues"/> are what its mapping validator
/// refused, located in the configuration, and make the save a 422. <paramref name="Warnings"/> name what the
/// changes left inconsistent, such as links to a collection that is gone.
/// </summary>
public sealed record MigrationPlanSaveOutcome(ValidationIssue[] Issues, string[] Warnings);

/// <summary>Turns the registered collections of the plan into the configuration the wizard carries on with.</summary>
public sealed record MigrationApplyRequest(string Slug);

public sealed record MigrationApplyResponse(
    CdcSinkConfiguration? Configuration,
    string[] UnmappedTables,
    string[] Errors);

public sealed record NamingConventions(
    PropertyCase PropertyCase = PropertyCase.Unspecified,
    string? PropertyLanguage = null,
    string? Notes = null);

public enum PropertyCase
{
    Unspecified,
    SnakeCase,
    CamelCase,
    PascalCase
}
