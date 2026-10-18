namespace Raven.Quill.Contracts;

/// One table the operator picked on the verify step, identified the way the discover response identifies it.
public sealed record SelectedSourceTable(string SourceTableName, string? SourceTableSchema = null);
