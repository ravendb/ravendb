namespace Raven.Server.SqlMigration.MsSQL
{
    internal sealed class MsSqlSchemaQueries : SqlSchemaQueries
    {
        public override string SelectColumnsQuery { get; } =
            "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE" +
            " FROM INFORMATION_SCHEMA.COLUMNS C JOIN INFORMATION_SCHEMA.TABLES T " +
            " ON C.TABLE_CATALOG = T.TABLE_CATALOG AND C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME " +
            " WHERE T.TABLE_TYPE <> 'VIEW' ";

        public override string SelectPrimaryKeysQuery { get; } =
            "SELECT TC.TABLE_SCHEMA, TC.TABLE_NAME, COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
            "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
            "ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME" +
            " ORDER BY ORDINAL_POSITION";

        public override string SelectReferentialConstraintsQuery { get; } =
            "SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME " +
            "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS";

        public override string SelectKeyColumnUsageQuery { get; } =
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME" +
            " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
            "ORDER BY ORDINAL_POSITION";
    }
}
