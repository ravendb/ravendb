using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

/// <summary>
/// One source-side table as the CDC schema-discovery endpoint sees it.
/// Field names mirror <see cref="CdcSinkTableConfig"/> so Studio can populate
/// the CDC mapping form with minimal transformation.
/// </summary>
internal class CdcSinkSourceTable : IDynamicJson
{
    /// <summary>
    /// SQL schema. Matches <see cref="CdcSinkTableConfig.SourceTableSchema"/>.
    /// </summary>
    public string SourceTableSchema { get; set; }

    /// <summary>
    /// SQL table name. Matches <see cref="CdcSinkTableConfig.SourceTableName"/>.
    /// </summary>
    public string SourceTableName { get; set; }

    public List<CdcSinkSourceColumn> Columns { get; set; } = new();

    public List<string> PrimaryKeyColumns { get; set; } = new();

    /// <summary>
    /// Foreign keys leaving this table. Studio uses these to suggest
    /// <see cref="CdcSinkLinkedTableConfig"/> entries.
    /// </summary>
    public List<CdcSinkSourceForeignKey> ForeignKeys { get; set; } = new();

    /// <summary>
    /// True when CDC tracking is active for this table at the source.
    /// SQL Server: present in <c>cdc.change_tables</c>. PostgreSQL / MySQL: always true
    /// (publication / binlog membership is a database-level concern; surfaced elsewhere).
    /// </summary>
    public bool IsCdcEnabled { get; set; }

    /// <summary>
    /// Set when the entire table cannot be CDC-captured. Null on usable tables.
    /// </summary>
    public string UnsupportedReason { get; set; }

    /// <summary>
    /// Non-fatal, table-scoped verification findings that don't make the table unusable but
    /// affect captured data quality — e.g. PostgreSQL REPLICA IDENTITY that won't carry
    /// row-identifying columns on DELETE.
    /// </summary>
    public List<string> Warnings { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(SourceTableSchema)] = SourceTableSchema,
            [nameof(SourceTableName)] = SourceTableName,
            [nameof(Columns)] = new DynamicJsonArray(Columns.Select(c => c.ToJson())),
            [nameof(PrimaryKeyColumns)] = new DynamicJsonArray(PrimaryKeyColumns),
            [nameof(ForeignKeys)] = new DynamicJsonArray(ForeignKeys.Select(fk => fk.ToJson())),
            [nameof(IsCdcEnabled)] = IsCdcEnabled,
            [nameof(UnsupportedReason)] = UnsupportedReason,
            [nameof(Warnings)] = new DynamicJsonArray(Warnings),
        };
    }
}
