using Raven.Client.Documents.Operations.ETL.SQL;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Schema;

/// <summary>
/// Body of <c>POST /admin/cdc-sink/schema</c>. The endpoint surfaces the source database's
/// tables, columns, PKs, and FKs annotated with CDC-specific hints so Studio (or a .NET
/// client) can render the mapping UI before saving a CDC task.
/// </summary>
public class CdcSinkSchemaRequest : IDynamicJson
{
    /// <summary>
    /// Inline credentials. Required path for Studio's Task Creation view, where the user
    /// is editing the connection but hasn't saved it to <c>databaseRecord.SqlConnectionStrings</c> yet.
    /// When null, falls back to <see cref="ConnectionStringName"/>.
    /// </summary>
    public SqlConnectionString Connection { get; set; }

    /// <summary>
    /// Optional fallback for post-save callers. Ignored when <see cref="Connection"/> is populated.
    /// </summary>
    public string ConnectionStringName { get; set; }

    /// <summary>
    /// Provider-specific schema filter. Currently only consumed by PostgreSQL (defaults to
    /// <c>["public"]</c> when null/empty).
    /// </summary>
    public string[] Schemas { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Connection)] = Connection?.ToJson(),
            [nameof(ConnectionStringName)] = ConnectionStringName,
            [nameof(Schemas)] = Schemas == null ? null : new DynamicJsonArray(Schemas),
        };
    }
}
