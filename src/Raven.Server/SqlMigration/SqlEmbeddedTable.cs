using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.SqlMigration
{
    public class SqlEmbeddedTable : SqlTable
    {
        private List<string> _parentColumns;
        public readonly string ParentTableName;

        public SqlEmbeddedTable(string tableName, string query, SqlDatabase database, string newName, string parentTable) : base(tableName, query, database, newName)
        {
            ParentTableName = parentTable;
            IsEmbedded = true;
        }

        public List<string> GetColumnsReferencingParentTable()
        {
            if (_parentColumns != null)
                return _parentColumns;

            var lst = (from item in ForeignKeys
                where item.Value == ParentTableName
                select item.Key).ToList();

            _parentColumns = lst;
            return lst;
        }

        public override SqlReader GetReader()
        {
            if (Reader != null && Reader.Disposed == false)
                return Reader;

            var query = InitialQuery + SqlQueries.OrderByColumns(GetColumnsReferencingParentTable());
            Reader = new SqlReader(Database.ConnectionString, query);
            Reader.ExecuteReader();
            return Reader;
        }

        public SqlReader GetReaderWhere(List<string> values)
        {
            var query = SqlQueries.SelectFromQueryWhere(InitialQuery, GetColumnsReferencingParentTable(), values) + SqlQueries.OrderByColumns(GetColumnsReferencingParentTable());

            if (Reader != null && Reader.Disposed == false)
            {
                Reader.SetCommand(query);
                return Reader;
            }

            Reader = new SqlReader(Database.ConnectionString, query);
            Reader.ExecuteReader();
            return Reader;
        }
    }
}
