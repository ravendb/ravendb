using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace Raven.Server.SqlMigration.MySQL
{
    internal sealed class MySqlSchemaQueries : SqlSchemaQueries
    {
        public override string SelectColumnsQuery { get; } =
            "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE, C.EXTRA " +
            " FROM INFORMATION_SCHEMA.COLUMNS C JOIN INFORMATION_SCHEMA.TABLES T " +
            " ON C.TABLE_CATALOG = T.TABLE_CATALOG AND C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME " +
            " WHERE C.TABLE_SCHEMA = @schema AND T.TABLE_TYPE <> 'VIEW' ";

        public override string SelectPrimaryKeysQuery { get; } =
            "SELECT TABLE_NAME, COLUMN_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
            "WHERE TABLE_SCHEMA = @schema AND CONSTRAINT_NAME = 'PRIMARY' " +
            "ORDER BY ORDINAL_POSITION";

        public override string SelectReferentialConstraintsQuery { get; } =
            "SELECT CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_NAME, REFERENCED_TABLE_NAME " +
            "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS " +
            "WHERE UNIQUE_CONSTRAINT_SCHEMA = @schema ";

        public override string SelectKeyColumnUsageQuery { get; } =
            "SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME " +
            " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
            " WHERE TABLE_SCHEMA = @schema AND CONSTRAINT_NAME <> 'PRIMARY' " +
            " ORDER BY ORDINAL_POSITION";

        protected internal override void AddSchemaParameter(DbCommand cmd, DbConnection connection)
        {
            var schemaParameter = cmd.CreateParameter();
            schemaParameter.ParameterName = "schema";
            schemaParameter.Value = connection.Database;
            cmd.Parameters.Add(schemaParameter);
        }

        /// <summary>
        /// Returns every column of a single table in ordinal-position order. MySQL fills
        /// <see cref="SqlColumnInfo.DetailedType"/> from COLUMN_TYPE (e.g. <c>"tinyint(1) unsigned"</c>)
        /// so binlog setup can distinguish boolean shapes. Binlog row events don't carry column
        /// names, so the CDC streaming path uses this list to map positional binlog values to
        /// named columns.
        /// </summary>
        public static async Task<List<SqlColumnInfo>> FetchTableColumnsAsync(
            MySqlConnection connection, string schema, string tableName, CancellationToken ct)
        {
            var columns = new List<SqlColumnInfo>();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
                ORDER BY ORDINAL_POSITION";
            cmd.Parameters.AddWithValue("@schema", schema);
            cmd.Parameters.AddWithValue("@table", tableName);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader.GetString(0);
                var dataType = reader.GetString(1).ToLowerInvariant();
                var detailedType = reader.GetString(2).ToLowerInvariant();
                columns.Add(new SqlColumnInfo(name, dataType, detailedType));
            }

            return columns;
        }
    }
}
