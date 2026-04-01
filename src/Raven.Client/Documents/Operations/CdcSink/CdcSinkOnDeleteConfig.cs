using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

/// <summary>
/// Controls how DELETE events are handled for a CDC Sink table (root or embedded).
/// When null (the default), DELETE events are processed normally — root documents are deleted,
/// embedded items are removed from the parent's array/map/value.
/// </summary>
public class CdcSinkOnDeleteConfig : IFillFromBlittableJson, IDynamicJson
{
    /// <summary>
    /// Optional JavaScript patch that runs when a DELETE event is received.
    ///
    /// For root tables: this = the existing document, $row = raw CDC row (DELETE event data).
    /// For embedded tables: this = the parent document, $row = the embedded row's DELETE event data.
    ///
    /// After the patch runs, the delete still proceeds by default. To cancel the delete
    /// and keep the document/item, return true from the script.
    ///
    /// Examples:
    ///
    ///   Archive pattern — cancel the delete, mark the document:
    ///     Patch = "this.Archived = true; this.ArchivedAt = new Date().toISOString(); return true;"
    ///
    ///   Audit pattern — log the deletion, then let it proceed:
    ///     Patch = "this.DeleteCount = (this.DeleteCount || 0) + 1;"
    ///     // no return true → delete proceeds normally
    ///
    ///   Embedded archive — mark the item inactive, keep it in the array:
    ///     Patch = @"
    ///       var lines = this.Lines || [];
    ///       for (var i = 0; i &lt; lines.length; i++) {
    ///         if (lines[i].LineNum == $row.line_num) {
    ///           lines[i].Deleted = true;
    ///         }
    ///       }
    ///       return true; // cancel the delete — keep the item in the array"
    /// </summary>
    public string Patch { get; set; }

    /// <summary>
    /// When true, DELETE events are silently ignored — no deletion occurs and no patch runs.
    /// This is useful for append-only data (e.g., audit logs) or when the embedded table's
    /// primary key doesn't include the join column to the parent and you don't want to set
    /// up REPLICA IDENTITY FULL on the source table.
    ///
    /// When IgnoreDeletes is true, the CDC process does not need the join column to be
    /// present in DELETE events, so the default REPLICA IDENTITY (primary key only) is
    /// sufficient regardless of whether the PK includes the join column.
    /// </summary>
    public bool IgnoreDeletes { get; set; }

    public void FillFromBlittableJson(BlittableJsonReaderObject json)
    {
        var config = DocumentConventions.Default.Serialization.DefaultConverter
            .FromBlittable<CdcSinkOnDeleteConfig>(json, "CdcSinkOnDeleteConfig");

        Patch = config.Patch;
        IgnoreDeletes = config.IgnoreDeletes;
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Patch)] = Patch,
            [nameof(IgnoreDeletes)] = IgnoreDeletes,
        };
    }
}
