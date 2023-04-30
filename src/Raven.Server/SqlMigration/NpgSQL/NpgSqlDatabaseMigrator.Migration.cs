using System.Linq;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.NpgSQL
{
    internal partial class NpgSqlDatabaseMigrator : GenericDatabaseMigrator
    {
        protected override string FactoryName => "Npgsql";

        private const string SelectColumnsTemplate = "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE" +
                                             " FROM INFORMATION_SCHEMA.COLUMNS C JOIN INFORMATION_SCHEMA.TABLES T " +
                                             " ON C.TABLE_CATALOG = T.TABLE_CATALOG AND C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME " +
                                             " WHERE T.TABLE_TYPE <> 'VIEW' AND T.TABLE_SCHEMA IN ({0})";

        private readonly string _selectColumns;

        public NpgSqlDatabaseMigrator(string connectionString, string[] schemas) : base(connectionString)
        {
            const string defaultSchema = "'public'";
            var schemasString = schemas == null || schemas.Length == 0 ? defaultSchema : string.Join(',', schemas.Select(x => $"'{x}'"));

            _selectColumns = string.Format(SelectColumnsTemplate, schemasString);
        }

        protected override string QuoteColumn(string columnName)
        {
            return $"{columnName}";
        }

        protected override string QuoteTable(string schema, string tableName)
        {
            return $"{schema}.{tableName}";
        }
        
        private ColumnType MapColumnType(string type)
        {
            type = type.ToLower();

            switch (type)
            {
                case "character varying":
                case "varchar":
                case "character":
                case "char":
                case "text":
                case "timestamp without time zone":
                case "timestamp with time zone":
                case "timestamp":
                case "timestamptz":
                case "date":
                case "time without time zone":
                case "time with time zone":
                case "time":
                case "timetz":
                case "interval":
                case "cidr":            
                case "inet":            
                case "macaddr":         
                case "macaddr8":        
                case "user-defined":     
                case "ENUM":
                    return ColumnType.String;

                case "smallint":
                case "int2":
                case "integer":
                case "int":
                case "int4":
                case "bigint":
                case "int8":
                case "decimal":
                case "numeric":
                case "real":
                case "float4":
                case "double precision":
                case "float8":
                case "smallserial":
                case "serial2":
                case "serial":
                case "serial4":
                case "bigserial":
                case "serial8":
                case "money":
                    return ColumnType.Number;

                case "bit":
                case "boolean":
                    return ColumnType.Boolean;

                case "bytea":
                    return ColumnType.Binary;

                case "array":
                    return ColumnType.Array;
                default:
                    return ColumnType.Unsupported;
            }
        }
        
        protected override string LimitRowsNumber(string inputQuery, int? rowsLimit)
        {
            if (rowsLimit.HasValue)
                return "select rowsLimited.* from (" + inputQuery + ") rowsLimited limit " + rowsLimit;

            return inputQuery;
        }

        protected override string GetSelectAllQueryForTable(string tableSchema, string tableName)
        {
            return "select * from " + QuoteTable(tableSchema, tableName);
        }
    }
}
