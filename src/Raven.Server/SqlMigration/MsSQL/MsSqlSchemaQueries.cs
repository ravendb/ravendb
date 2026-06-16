using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.CdcSink;

namespace Raven.Server.SqlMigration.MsSQL
{
    internal sealed class MsSqlSchemaQueries : SqlSchemaQueries
    {
        private const string SelectTableColumnsQuery = @"
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION";


        public override string SelectColumnsQuery { get; } =
            "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE" +
            " FROM INFORMATION_SCHEMA.COLUMNS C JOIN INFORMATION_SCHEMA.TABLES T " +
            " ON C.TABLE_CATALOG = T.TABLE_CATALOG AND C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME " +
            " WHERE T.TABLE_TYPE <> 'VIEW' ";

        public override string SelectPrimaryKeysQuery { get; } =
            "SELECT TC.TABLE_SCHEMA, TC.TABLE_NAME, COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
            "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
            // SQL Server constraint names are not database-wide unique; joining on CONSTRAINT_NAME
            // alone cross-joins identically-named PKs across schemas and attaches the wrong PK columns.
            // Qualify by schema + table.
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
        /// SQL Server has no equivalent of MySQL's COLUMN_TYPE, so <see cref="SqlColumnInfo.DetailedType"/>
        /// is set to the same value as <see cref="SqlColumnInfo.DataType"/>.
        /// </summary>
        public static async Task<List<SqlColumnInfo>> FetchTableColumnsAsync(
            DbConnection connection, string schema, string tableName, CancellationToken ct)
        {
            var columns = new List<SqlColumnInfo>();

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = SelectTableColumnsQuery;
            CdcSinkProcess.AddParameter(cmd, "@schema", schema);
            CdcSinkProcess.AddParameter(cmd, "@table", tableName);

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
