using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

/// <summary>
/// Controls how DELETE events are handled for a CDC Sink table (root or embedded).
/// When null (the default), DELETE events are processed normally — root documents are deleted,
/// embedded items are removed from the parent's array/map/value.
/// </summary>
public class CdcSinkOnDeleteConfig : IDynamicJson
{
    /// <summary>
    /// Optional JavaScript patch that runs when a DELETE event is received.
    ///
    /// For root tables: this = the existing document, $row = raw CDC row (DELETE event data).
    /// For embedded tables: this = the parent document, $row = the embedded row's DELETE event data.
    ///
    /// The patch runs before the delete is applied. Whether the delete proceeds afterward
    /// depends on the IgnoreDeletes flag:
    ///   - IgnoreDeletes = false (default): patch runs, then delete proceeds.
    ///   - IgnoreDeletes = true: patch runs, delete is skipped.
    ///
    /// Examples:
    ///
    ///   Audit trail — write a record to a separate audit document, then let the delete proceed:
    ///     OnDelete = new CdcSinkOnDeleteConfig
    ///     {
    ///         Patch = @"put('AuditLog/' + new Date().getTime(), {
    ///             Action: 'Deleted',
    ///             DocumentId: id(this),
    ///             DeletedAt: new Date().toISOString(),
    ///             LastKnownName: this.Name
    ///         });"
    ///     }
    ///
    ///   Archive pattern — mark the document, prevent deletion:
    ///     OnDelete = new CdcSinkOnDeleteConfig
    ///     {
    ///         IgnoreDeletes = true,
    ///         Patch = "this.Archived = true; this.ArchivedAt = new Date().toISOString();"
    ///     }
    ///
    ///   Conditional delete — only delete sent orders, keep the rest:
    ///     OnDelete = new CdcSinkOnDeleteConfig
    ///     {
    ///         IgnoreDeletes = true,
    ///         Patch = @"
    ///           if (this.Status !== 'Sent') {
    ///             // Order hasn't been sent yet — keep it (IgnoreDeletes applies)
    ///             return;
    ///           }
    ///           // Order was sent — explicitly delete it despite IgnoreDeletes
    ///           del(id(this));"
    ///     }
    ///
    ///   Snapshot before delete — capture last known state on the parent:
    ///     OnDelete = new CdcSinkOnDeleteConfig
    ///     {
    ///         Patch = "this.LastDeletedLineProduct = $row.product;"
    ///     }
    /// </summary>
    public string Patch { get; set; }

    /// <summary>
    /// When true, the DELETE operation is not applied — the document/item is kept.
    /// If a Patch is also set, the patch runs first, then the delete is skipped.
    /// If no Patch is set, the DELETE event is silently discarded.
    ///
    /// Use cases:
    /// - Archive pattern: set IgnoreDeletes = true with a Patch that marks the
    ///   document as archived (e.g., setting an Archived flag).
    /// - Append-only data (e.g., audit logs) where rows should never be removed.
    /// - When the embedded table's primary key doesn't include the join column to
    ///   the parent and you don't want to set up REPLICA IDENTITY FULL (PostgreSQL-specific;
    ///   SQL Server CDC always includes all tracked columns in change rows).
    ///
    /// When IgnoreDeletes is true (without a Patch), the CDC process does not need
    /// the join column to be present in DELETE events, so for PostgreSQL the default
    /// REPLICA IDENTITY (primary key only) is sufficient regardless of whether the PK
    /// includes the join column.
    /// </summary>
    public bool IgnoreDeletes { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Patch)] = Patch,
            [nameof(IgnoreDeletes)] = IgnoreDeletes,
        };
    }
}
