namespace Raven.Client.Documents.Operations.CdcSink.Test;

public enum TestCdcSinkRowSelector
{
    /// <summary>Sample the first N rows of the table, ordered by primary key.</summary>
    First,

    /// <summary>Fetch the single row whose primary-key values match the supplied list.</summary>
    ByPrimaryKey
}
