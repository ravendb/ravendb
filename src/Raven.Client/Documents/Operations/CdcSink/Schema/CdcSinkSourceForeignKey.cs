using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

/// <summary>
/// Foreign-key reference from one source table to another, surfaced by the
/// CDC schema-discovery endpoint so Studio can suggest linked tables for
/// the CDC mapping UI.
/// </summary>
internal class CdcSinkSourceForeignKey : IDynamicJson
{
    /// <summary>
    /// FK column(s) on the source ("child") table.
    /// </summary>
    public List<string> Columns { get; set; } = new();

    /// <summary>
    /// Schema of the referenced ("parent") table.
    /// </summary>
    public string ReferencedSchema { get; set; }

    /// <summary>
    /// Name of the referenced ("parent") table.
    /// </summary>
    public string ReferencedTable { get; set; }

    /// <summary>
    /// PK column(s) on the referenced table.
    /// </summary>
    public List<string> ReferencedColumns { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Columns)] = new DynamicJsonArray(Columns),
            [nameof(ReferencedSchema)] = ReferencedSchema,
            [nameof(ReferencedTable)] = ReferencedTable,
            [nameof(ReferencedColumns)] = new DynamicJsonArray(ReferencedColumns),
        };
    }
}
