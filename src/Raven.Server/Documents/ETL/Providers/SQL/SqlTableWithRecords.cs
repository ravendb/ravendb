using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlTableWithRecords : SqlEtlTable
    {
        public SqlTableWithRecords(SqlEtlTable table)
        {
            TableName = table.TableName;
            DocumentIdColumn = table.DocumentIdColumn;
            InsertOnlyMode = table.InsertOnlyMode;
        }

        public readonly List<ToSqlItem> Inserts = new List<ToSqlItem>();

        public readonly List<ToSqlItem> Deletes = new List<ToSqlItem>();
    }
}