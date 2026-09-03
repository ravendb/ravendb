namespace Raven.Quill.Wizard;

public sealed record VerifyCdcRequest(
    VerifyCdcTableRequest[] Tables,
    string Slug = "");

public sealed record VerifyCdcTableRequest(string SourceTableName, string? SourceTableSchema = null);
