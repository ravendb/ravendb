using System.Collections.Generic;

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
    /// All column values from the CDC message or initial load row.
    /// For INSERT/UPDATE: all columns with new values.
    /// For DELETE: at least primary key columns (may have all columns with REPLICA IDENTITY FULL).
    /// Keys are the original SQL column names.
    /// </summary>
    public Dictionary<string, object> Data { get; set; }
}

public enum CdcSinkOperation
{
    Upsert,
    Delete
}
