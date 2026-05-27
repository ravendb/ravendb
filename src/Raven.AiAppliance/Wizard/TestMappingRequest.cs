namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Body of <c>POST /api/setup/test-mapping</c>. The wizard reads the full
/// CDC configuration back from <c>wizard-state.LastMapConfiguration</c>, so
/// the caller only supplies which source table to probe + how many rows to
/// sample. <c>RowSelector</c> and <c>Operation</c> are hard-coded to
/// <c>First</c> / <c>Upsert</c> in the handler — by-PK and delete previews
/// aren't part of the wizard surface yet.
/// </summary>
public sealed record TestMappingRequest(
    string SourceTableName,
    int? MaxRows = null,
    string? SourceTableSchema = null);
