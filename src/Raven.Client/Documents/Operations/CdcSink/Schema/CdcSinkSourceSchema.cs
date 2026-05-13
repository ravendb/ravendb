using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

/// <summary>
/// Top-level response of <c>POST /admin/cdc-sink/schema</c>. Carries the
/// source-side schema annotated with CDC-specific capturability hints so
/// Studio can drive the CDC mapping UI without a second round-trip.
/// </summary>
public class CdcSinkSourceSchema : IDynamicJson
{
    /// <summary>
    /// Source database / catalog name (whatever <c>connection.Database</c> returns
    /// for MS SQL / MySQL / PostgreSQL; the Oracle owner for Oracle — Oracle is not
    /// a CDC source today but the field stays generic).
    /// </summary>
    public string CatalogName { get; set; }

    public List<CdcSinkSourceTable> Tables { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(CatalogName)] = CatalogName,
            [nameof(Tables)] = new DynamicJsonArray(Tables.Select(t => t.ToJson())),
        };
    }
}
