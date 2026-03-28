using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// The result of processing a single CDC row through <see cref="CdcSinkDocumentProcessor"/>.
/// Describes what operation to perform on a RavenDB document.
/// </summary>
public class CdcSinkDocumentOp
{
    /// <summary>
    /// The type of document operation to perform.
    /// </summary>
    public CdcSinkDocumentOpType Type { get; set; }

    /// <summary>
    /// The target document ID.
    /// For root tables: the document to put/delete.
    /// For embedded tables: the parent document to modify.
    /// </summary>
    public string DocumentId { get; set; }

    /// <summary>
    /// The per-table processor context (shared across all rows for the same table, not allocated per row).
    /// Contains table config, collection name, path from root, etc.
    /// </summary>
    public CdcSinkTableProcessor Processor { get; set; }

    /// <summary>
    /// The column-mapped row data (SQL column names renamed to document property names).
    /// For Put operations: the full document content.
    /// For EmbeddedModify: the embedded item data.
    /// </summary>
    public DynamicJsonValue MappedData { get; set; }

    /// <summary>
    /// All columns from the CDC row (original SQL column names, including unmapped ones).
    /// Used for $row access in JS patches.
    /// </summary>
    public Dictionary<string, object> RawData { get; set; }

    /// <summary>
    /// The operation (Upsert/Delete) — relevant for embedded operations.
    /// </summary>
    public CdcSinkOperation Operation { get; set; }
}

public enum CdcSinkDocumentOpType
{
    /// <summary>
    /// Put (upsert) a root document.
    /// </summary>
    Put,

    /// <summary>
    /// Delete a root document.
    /// </summary>
    Delete,

    /// <summary>
    /// Modify an embedded item within a parent document (insert, update, or remove from array/map/value).
    /// </summary>
    EmbeddedModify
}
