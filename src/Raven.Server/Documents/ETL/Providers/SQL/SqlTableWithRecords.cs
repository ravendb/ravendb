using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    public class SqlTableWithRecords : SqlReplicationTable
    {
        public SqlTableWithRecords(SqlReplicationTable table)
        {
            TableName = table.TableName;
            DocumentKeyColumn = table.DocumentKeyColumn;
            InsertOnlyMode = table.InsertOnlyMode;
        }

        public readonly List<ToSqlItem> Inserts = new List<ToSqlItem>();

        public readonly List<ToSqlItem> Deletes = new List<ToSqlItem>();
    }
}