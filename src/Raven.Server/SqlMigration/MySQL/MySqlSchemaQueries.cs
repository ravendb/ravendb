using System.Data.Common;

namespace Raven.Server.SqlMigration.MySQL
{
    internal sealed class MySqlSchemaQueries : SqlSchemaQueries
    {
        public override string SelectColumnsQuery { get; } =
            "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE " +
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
    }
}
