using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Raven.Server.SqlMigration.NpgSQL
{
    internal sealed class NpgSqlSchemaQueries : SqlSchemaQueries
    {
        private const string SelectTableColumnsQuery =
            @"SELECT column_name, data_type FROM information_schema.columns
              WHERE table_schema = @schema AND table_name = @table
              ORDER BY ordinal_position";


        private const string SelectColumnsTemplate =
            "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE" +
            " FROM INFORMATION_SCHEMA.COLUMNS C JOIN INFORMATION_SCHEMA.TABLES T " +
            " ON C.TABLE_CATALOG = T.TABLE_CATALOG AND C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME " +
            " WHERE T.TABLE_TYPE <> 'VIEW' AND T.TABLE_SCHEMA IN ({0})";

        public NpgSqlSchemaQueries(string[] schemas)
        {
            const string defaultSchema = "'public'";
            var schemasString = schemas == null || schemas.Length == 0 ? defaultSchema : string.Join(',', schemas.Select(x => $"'{x}'"));

            SelectColumnsQuery = string.Format(SelectColumnsTemplate, schemasString);
        }

        public override string SelectColumnsQuery { get; }

        public override string SelectPrimaryKeysQuery { get; } =
            "SELECT TC.TABLE_SCHEMA, TC.TABLE_NAME, COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
            "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
            // Postgres constraint names are unique only within (catalog, schema); joining on
            // CONSTRAINT_NAME alone cross-joins identically-named PKs across schemas (e.g. customers_pkey
            // in both public and tenant_a) and attaches the wrong PK columns. Qualify by schema + table.
            "ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
            "AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME " +
            "AND TC.CONSTRAINT_SCHEMA = KU.CONSTRAINT_SCHEMA " +
            "AND TC.TABLE_SCHEMA = KU.TABLE_SCHEMA " +
            "AND TC.TABLE_NAME = KU.TABLE_NAME" +
            " ORDER BY ORDINAL_POSITION";

        public override string SelectReferentialConstraintsQuery { get; } =
            "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, UNIQUE_CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_NAME " +
            "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS";

        public override string SelectKeyColumnUsageQuery { get; } =
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME" +
            " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
            "ORDER BY ORDINAL_POSITION";

        /// <summary>
        /// Returns every column of a single table in ordinal-position order.
        /// Shared between the CDC streaming path (BindKeysetParameters caches it per table
        /// and projects to a PK-name → data_type dict) and the future schema-discovery endpoint.
        /// Postgres has no equivalent of MySQL's COLUMN_TYPE, so <see cref="SqlColumnInfo.DetailedType"/>
        /// is set to the same value as <see cref="SqlColumnInfo.DataType"/>.
        /// </summary>
        public static async Task<List<SqlColumnInfo>> FetchTableColumnsAsync(
            NpgsqlConnection connection, string schema, string tableName, CancellationToken ct)
        {
            var columns = new List<SqlColumnInfo>();

            await using var cmd = new NpgsqlCommand(SelectTableColumnsQuery, connection);
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", tableName);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var dataType = reader.GetString(1).ToLowerInvariant();
                columns.Add(new SqlColumnInfo(name, dataType, dataType));
            }

            return columns;
        }
    }
}
