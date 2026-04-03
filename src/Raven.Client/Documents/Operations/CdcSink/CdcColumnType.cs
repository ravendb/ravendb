namespace Raven.Client.Documents.Operations.CdcSink;

/// <summary>
/// Controls how a CDC Sink column is stored in the target RavenDB document.
/// </summary>
public enum CdcColumnType
{
    /// <summary>
    /// Store as a document property with standard type conversion.
    /// int/smallint/bigint → long, real/float/double → double, numeric/decimal → double,
    /// boolean → bool, date → DateOnly, timestamp/timestamptz → DateTime,
    /// uuid → string, varchar/text → string, arrays → JSON arrays.
    /// JSON/JSONB columns are stored as plain strings unless explicitly marked as Json.
    /// </summary>
    Default,

    /// <summary>
    /// Parse the string value as its native JSON type in the document.
    /// Handles all JSON value types: objects, arrays, strings, numbers, booleans,
    /// and null. Use for json/jsonb columns in PostgreSQL, or nvarchar(max) with
    /// JSON content in SQL Server. Without this type, JSON values are stored as
    /// escaped strings.
    /// </summary>
    Json,

    /// <summary>
    /// Store as a RavenDB attachment instead of a document property.
    /// The binary format depends on the CLR type at runtime:
    /// byte[] → binary (application/octet-stream),
    /// string → UTF-8 text (text/plain),
    /// float[]/double[] → raw vector data (application/octet-stream).
    /// </summary>
    Attachment
}
