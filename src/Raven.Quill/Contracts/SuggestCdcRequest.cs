namespace Raven.Quill.Contracts;

public sealed record SuggestCdcRequest(string? IntentPrompt, SelectedSourceTable[] SelectedTables, string Slug = "");

/// One table the operator picked on the verify step, identified the way the discover response identifies it.
public sealed record SelectedSourceTable(string SourceTableName, string? SourceTableSchema = null);
