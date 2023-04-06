using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.MySQL
{
    internal partial class MySqlDatabaseMigrator : GenericDatabaseMigrator
    {
        private readonly string _factoryName;
        protected override string FactoryName => _factoryName;

        public MySqlDatabaseMigrator(string connectionString, string factoryName) : base(connectionString)
        {
            _factoryName = factoryName;
        }

        protected override string QuoteColumn(string columnName)
        {
            return $"`{columnName}`";
        }

        protected override string QuoteTable(string schema, string tableName)
        {
            return $"`{schema}`.`{tableName}`";
        }
        
        private ColumnType MapColumnType(string type)
        {
            type = type.ToLower();

            switch (type)
            {
                case "varchar":
                case "longtext":
                case "enum":
                case "text":
                case "timestamp":
                case "char":
                case "set":
                case "mediumtext":
                case "time":
                case "date":
                case "datetime":
                case "tinytext":
                    return ColumnType.String;

                case "bigint":
                case "int":
                case "tinyint":
                case "decimal":
                case "double":
                case "float":
                case "real":
                case "year":
                case "smallint":
                case "mediumint":
                    return ColumnType.Number;

                case "bit":
                    return ColumnType.Boolean;

                case "tinyblob":
                case "blob":
                case "mediumblob":
                case "longblob":
                case "binary":
                case "varbinary":
                    return ColumnType.Binary;

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
