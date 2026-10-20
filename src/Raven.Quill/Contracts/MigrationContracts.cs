using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Quill.Contracts;

/// <summary>Begins a planning session over the schema the wizard already discovered for this app.</summary>
public sealed record MigrationStartRequest(
    string Slug,
    SelectedSourceTable[]? SelectedTables = null,
    string? Prompt = null);

/// <summary>
/// One more turn in an existing planning session. <paramref name="SelectedAreas"/> and
/// <paramref name="SelectedCollections"/> are what the operator picked rather than wrote; with a selection the
/// prompt may be empty.
/// </summary>
public sealed record MigrationAskRequest(
    string Slug,
    string ConversationId,
    string? Prompt = null,
    string[]? SelectedAreas = null,
    string[]? SelectedCollections = null);

/// <summary>
/// The operator's change to one collection of the plan. A null <paramref name="Config"/> deletes it; a config
/// whose CollectionName differs from <paramref name="Collection"/> renames it; a collection the plan does not
/// hold is added. The planner is told on the next turn.
/// </summary>
public sealed record MigrationEditCollectionRequest(string Slug, string ConversationId, string Collection, CdcSinkTableConfig? Config = null);

/// <summary><paramref name="Warnings"/> name what the change left inconsistent, such as links to a collection that is gone.</summary>
public sealed record MigrationEditCollectionResponse(string[] Warnings);

/// <summary>
/// Turns what the session registered into the configuration the wizard carries on with.
/// <paramref name="Collections"/> narrows it to the ones the operator kept; empty or absent takes
/// the whole plan. Anything left out is reported back in the coverage diff, not silently dropped.
/// </summary>
public sealed record MigrationApplyRequest(string Slug, string ConversationId, string[]? Collections = null);

public sealed record MigrationApplyResponse(
    CdcSinkConfiguration? Configuration,
    string[] UnmappedTables,
    string[] Errors);
