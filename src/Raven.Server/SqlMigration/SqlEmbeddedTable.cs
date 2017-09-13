using System;
using System.Collections.Generic;
using System.Data;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.SqlMigration
{
    public class SqlEmbeddedTable : SqlTable
    {
        private List<string> _parentColumns;
        private readonly string _parentTableName;
        public readonly string PropertyName;

        public SqlEmbeddedTable(string tableName, string query, string patch, string parentTable, string property, IDbConnection connection) : base(tableName, query, patch, connection)
        {
            PropertyName = string.IsNullOrWhiteSpace(property) ? tableName : property;
            _parentTableName = parentTable;

            IsEmbedded = true;
        }

        public List<string> GetColumnsReferencingParentTable()
        {
            if (_parentColumns != null)
                return _parentColumns;

            var lst = new List<string>();

            foreach (var item in ForeignKeys)
                if (item.Value == _parentTableName)
                    lst.Add(item.Key);

            _parentColumns = lst;
            return lst;
        }

        public new SqlReader GetReader()
        {
            if (Reader != null)
                return Reader;

            var query = InitialQuery + SqlQueries.OrderByColumns(GetColumnsReferencingParentTable());
            Reader = new SqlReader(Connection, query, true);
            Reader.ExecuteReader();
            return Reader;
        }

        public SqlReader GetReaderWhere(List<string> values)
        {
            var query = SqlQueries.SelectFromQueryWhere(InitialQuery, GetColumnsReferencingParentTable(), values) + SqlQueries.OrderByColumns(GetColumnsReferencingParentTable());

            if (Reader != null)
            {
                Reader.SetCommand(query);
                return Reader;
            }

            Reader = new SqlReader(Connection, query, true);
            Reader.ExecuteReader();
            return Reader;
        }

        public static explicit operator SqlEmbeddedTable(List<string> v)
        {
            throw new NotImplementedException();
        }
    }
}
