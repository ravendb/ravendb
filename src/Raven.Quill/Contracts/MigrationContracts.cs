using System.Text.Json.Serialization;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.Contracts;

public sealed record MigrationProposalStartRequest(
    string Slug,
    SelectedSourceTable[]? SelectedTables = null,
    string? Prompt = null);

public sealed record MigrationProposalAskRequest(string Slug, string Prompt);

public sealed record MigrationAreaStartRequest(string Slug, string Area);

public sealed record MigrationAreaAskRequest(string Slug, string Area, string Prompt);

public enum PlannerAgent
{
    Proposal,
    Area
}

public sealed record MigrationStopRequest(string Slug, [property: JsonRequired] PlannerAgent Agent, string? Area = null);

public sealed record MigrationPlanGetRequest(string Slug);

public sealed record MigrationPlanGetResponse(MigrationPlanDocument Plan, MigrationProposal? Proposal);

public sealed class MigrationPlanDocument
{
    public string Id { get; set; } = string.Empty;

    public string? LicenseId { get; set; }

    public string? CustomerId { get; set; }

    public string? Slug { get; set; }

    public NamingConventions Conventions { get; set; } = new();

    public Dictionary<string, PlanEntry> Entries { get; set; } = new();
}

public sealed record MigrationPlanSaveRequest(string Slug, MigrationPlanChange[] Changes);

public enum PlanChangeKind
{
    Edit,
    Rename,
    Move,
    Delete
}

public sealed record MigrationPlanChange(
    PlanChangeKind Kind,
    string Area,
    string Collection,
    CdcSinkTableConfig? Config = null,
    string? NewName = null,
    string? MoveToArea = null);

public sealed record MigrationPlanSaveOutcome(ValidationIssue[] Issues, string[] Warnings);

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
