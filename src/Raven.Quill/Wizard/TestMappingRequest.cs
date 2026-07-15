namespace Raven.Quill.Wizard;

public sealed record TestMappingRequest(
    string SourceTableName,
    int? MaxRows = null,
    string? SourceTableSchema = null);
