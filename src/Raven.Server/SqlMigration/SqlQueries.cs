using System.Collections.Generic;

namespace Raven.Server.SqlMigration
{
    public static class SqlQueries
    {
        public const string SelectAllTables = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = @tableType order by 'TABLE_SCHEMA','TABLE_NAME'";
        public const string SelectReferantialConstraints = "select CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME from information_schema.REFERENTIAL_CONSTRAINTS";
        public const string SelectKeyColumnUsageWhereConstraintName = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_NAME = @constraintName";
        public const string SelectPrimaryKeys = "select tc.TABLE_SCHEMA, tc.TABLE_NAME, COLUMN_NAME from information_schema.TABLE_CONSTRAINTS as tc inner join information_schema.KEY_COLUMN_USAGE as ku on tc.CONSTRAINT_TYPE = 'PRIMARY KEY' and tc.constraint_name = ku.CONSTRAINT_NAME";
        public const string SelectColumns = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from information_schema.COLUMNS";

        private const string DefaultTableQuery = "select * from {0}";
        private const string SelectSingleRow = "select top 1 * from ({0}) a";
        private const string SelectQueryWhere = "select * from ({0}) a where {1}";
        private const string And = " and ";
        private const string Or = " or ";
        private const string OrderBy = " order by ";
        private const string EqualSign = " = ";

        public static string SelectTable(string tableName)
        {
            return string.Format(DefaultTableQuery, TableQuote(tableName));
        }

        private static string TableQuote(string s)
        {
            s = $"[{s}]";

            if (s.Contains(".") == false)
                return s;

            s = s.Insert(s.IndexOf('.'), "]");
            s = s.Insert(s.IndexOf('.') + 1, "[");

            return s;
        }

        public static string OrderByColumns(List<string> columns)
        {
            if (columns.Count == 0)
                return string.Empty;

            return OrderBy + string.Join(",", columns);
        }

        public static string SelectSingleRowFromQuery(string tableInitialQuery)
        {
            return string.Format(SelectSingleRow, tableInitialQuery);
        }

        public static string SelectFromQueryWhere(string tableInitialQuery, List<string> columns, List<string> values)
        {
            var where = string.Empty;

            for (var i = 0; i < columns.Count / values.Count; i++)
            {
                if (i != 0)
                    where += Or;

                for (var j = 0; j < values.Count; j++)
                {
                    if (j != 0)
                        where += And;

                    where += columns[i * values.Count + j] + EqualSign + values[j];
                }
            }

            return string.Format(SelectQueryWhere, tableInitialQuery, where);
        }
    }
}
