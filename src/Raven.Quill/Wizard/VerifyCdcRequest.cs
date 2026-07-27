namespace Raven.Quill.Wizard;

public sealed record VerifyCdcRequest(VerifyCdcTableRequest[] Tables);

public sealed record VerifyCdcTableRequest(string SourceTableName, string? SourceTableSchema = null);
