namespace Raven.Server.SqlMigration
{
    /// <summary>
    /// Single-table column metadata used by every provider's <c>FetchTableColumnsAsync</c>.
    /// </summary>
    /// <param name="Name">Column name, case-preserving.</param>
    /// <param name="DataType">Lowercased short type from INFORMATION_SCHEMA.COLUMNS.DATA_TYPE
    /// (e.g. <c>"varchar"</c>, <c>"tinyint"</c>, <c>"integer"</c>, <c>"jsonb"</c>).</param>
    /// <param name="DetailedType">Lowercased fuller type. On MySQL this is the value of
    /// COLUMN_TYPE (e.g. <c>"tinyint(1) unsigned"</c>) so binlog setup can distinguish boolean
    /// shapes. On Postgres / SQL Server the helpers return the same string as <see cref="DataType"/>
    /// for now — the field exists so callers can read one shape regardless of provider.</param>
    internal sealed record SqlColumnInfo(string Name, string DataType, string DetailedType);
}
