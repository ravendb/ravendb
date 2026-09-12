namespace Raven.Server.SqlMigration.Oracle
{
    internal sealed class OracleSchemaQueries : SqlSchemaQueries
    {
        public const string GetSchemaQuery = "select user_cons_columns.owner as TABLE_SCHEMA from user_cons_columns WHERE ROWNUM = 1";

        public override string SelectColumnsQuery { get; } =
            "SELECT (select user_cons_columns.owner from user_cons_columns WHERE ROWNUM = 1) as TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLS " +
            "where TABLE_NAME not in (select view_name from all_views where owner = (select user_cons_columns.owner from user_cons_columns WHERE ROWNUM = 1))";

        public override string SelectPrimaryKeysQuery { get; } =
            "select user_cons_columns.owner as TABLE_SCHEMA, user_cons_columns.table_name, user_cons_columns.column_name " +
            "from user_constraints, user_cons_columns " +
            "where user_constraints.constraint_type = 'P' " +
            "and user_constraints.constraint_name = user_cons_columns.constraint_name " +
            "and user_constraints.owner = user_cons_columns.owner " +
            "order by user_cons_columns.owner, user_cons_columns.table_name, user_cons_columns.position";

        public override string SelectReferentialConstraintsQuery { get; } =
            "select CONSTRAINT_NAME, R_CONSTRAINT_NAME AS \"UNIQUE_CONSTRAINT_NAME\" from user_constraints where CONSTRAINT_TYPE = 'R'";

        public override string SelectKeyColumnUsageQuery { get; } =
            "select user_constraints.owner as \"TABLE_SCHEMA\", user_constraints.TABLE_NAME, USER_CONS_COLUMNS.COLUMN_NAME, user_constraints.CONSTRAINT_NAME " +
            "from user_constraints inner join USER_CONS_COLUMNS on user_constraints.CONSTRAINT_NAME = USER_CONS_COLUMNS.CONSTRAINT_NAME " +
            "where user_constraints.constraint_TYPE = 'P' OR user_constraints.constraint_TYPE = 'R'";
    }
}
