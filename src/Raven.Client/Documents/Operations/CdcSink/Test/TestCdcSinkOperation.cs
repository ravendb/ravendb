namespace Raven.Client.Documents.Operations.CdcSink.Test;

internal enum TestCdcSinkOperation
{
    /// <summary>Drive each row through the per-table <c>Patch</c> script.</summary>
    Upsert,

    /// <summary>Drive each row through the <c>OnDelete.Patch</c> script and report whether CDC would delete the document.</summary>
    Delete
}
