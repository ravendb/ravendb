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

    /// <summary>
    /// Discovered source tables, each annotated with CDC capturability hints.
    /// </summary>
    public List<CdcSinkSourceTable> Tables { get; set; } = new();

    /// <summary>
    /// Whole-request failures (validation, missing connection-string, source DB unreachable) and
    /// connection-level verification blockers (e.g. PostgreSQL <c>wal_level</c> not <c>logical</c>,
    /// the connecting user lacking the privilege to provision CDC with no infrastructure in place).
    /// Per-table issues live on the individual <see cref="CdcSinkSourceTable.UnsupportedReason"/>
    /// / <see cref="CdcSinkSourceTable.Warnings"/> / column-level
    /// <see cref="CdcSinkSourceColumn.UnsupportedReason"/> fields instead.
    /// </summary>
    public List<string> Errors { get; set; } = new();

    /// <summary>
    /// Whether the connecting user has sufficient privileges to provision the CDC infrastructure
    /// (PostgreSQL replication slot/publication, SQL Server <c>sp_cdc_enable_*</c>). When false an
    /// administrator must set CDC up out-of-band. Distinct from per-table
    /// <see cref="CdcSinkSourceTable.IsCdcEnabled"/>, which reports whether CDC is already active.
    /// </summary>
    public bool HasPermissionToSetup { get; set; }

    /// <summary>
    /// Non-fatal connection-level verification findings (e.g. SQL Server Agent not running,
    /// CDC infrastructure already present under a reduced-privilege account). Per-table warnings
    /// live on <see cref="CdcSinkSourceTable.Warnings"/>.
    /// </summary>
    public List<string> Warnings { get; set; } = new();

    /// <summary>
    /// True when nothing blocks setting up CDC against this source — i.e. there are no
    /// <see cref="Errors"/>. Warnings do not affect success.
    /// </summary>
    public bool Success => Errors.Count == 0;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(CatalogName)] = CatalogName,
            [nameof(Tables)] = new DynamicJsonArray(Tables.Select(t => t.ToJson())),
            [nameof(Errors)] = new DynamicJsonArray(Errors),
            [nameof(HasPermissionToSetup)] = HasPermissionToSetup,
            [nameof(Warnings)] = new DynamicJsonArray(Warnings),
            [nameof(Success)] = Success,
        };
    }
}
