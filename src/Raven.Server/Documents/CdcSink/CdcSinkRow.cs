namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// Represents a single row from a CDC change event or initial load.
/// </summary>
public class CdcSinkRow
{
    /// <summary>
    /// SQL schema name (e.g., "public", "dbo").
    /// </summary>
    public string TableSchema { get; set; }

    /// <summary>
    /// SQL table name (e.g., "orders", "order_details").
    /// </summary>
    public string TableName { get; set; }

    /// <summary>
    /// The operation type: Upsert (insert or update) or Delete.
    /// </summary>
    public CdcSinkOperation Operation { get; set; }

    /// <summary>
    /// Column values from the CDC message or initial load row, in positional order
    /// matching the table's <see cref="CdcSinkTableProcessor.SourceColumnNames"/>.
    /// Indices correspond 1:1 with the processor's column name array.
    /// </summary>
    public object[] Data { get; set; }
}

public enum CdcSinkOperation
{
    Upsert,
    Delete
}
