using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.MsSQL
{
    internal partial class MsSqlDatabaseMigrator : GenericDatabaseMigrator
    {
        protected override string FactoryName => "System.Data.SqlClient";
        
        public MsSqlDatabaseMigrator(string connectionString) : base(connectionString)
        {
        }

        protected override string QuoteColumn(string columnName)
        {
            return $"[{columnName}]";
        }

        protected override string QuoteTable(string schema, string tableName)
        {
            return $"[{schema}].[{tableName}]";
        }

        private ColumnType MapColumnType(string type)
        {
            type = type.ToLower();

            switch (type)
            {
                case "bigint":
                case "decimal":
                case "float":
                case "real":
                case "int":
                case "numeric":
                case "smallint":
                case "smallmoney":
                case "tinyint":
                case "money":
                    return ColumnType.Number;
                case "bit":
                    return ColumnType.Boolean;
                case "binary":
                case "varbinary":
                case "image":
                    return ColumnType.Binary;
                case "char":
                case "nchar":
                case "ntext":
                case "nvarchar":
                case "smalldatetime":
                case "datetime":
                case "datetime2":
                case "datetimeoffset":
                case "date":
                case "time":
                case "text":
                case "timestamp":
                case "uniqueidentifier":
                case "varchar":
                case "xml":
                    return ColumnType.String;

                default:
                    return ColumnType.Unsupported;
            }
        }

        protected override string LimitRowsNumber(string inputQuery, int? rowsLimit)
        {
            if (rowsLimit.HasValue) 
                return "select top " + rowsLimit + " rowsLimited.* from (" + inputQuery + ") rowsLimited";
            
            return inputQuery;
        }

        
        protected override string GetSelectAllQueryForTable(string tableSchema, string tableName)
        {
            return "select * from " + QuoteTable(tableSchema, tableName);
        }
    }
}
