using System;
using System.Collections.Generic;
using System.Data;

namespace Raven.Server.SqlMigration
{
    public class SqlTable
    {

        public string Name { get; }
        public List<string> PrimaryKeys { get; }
        public Dictionary<string, string> ForeignKeys { get; }
        public string InitialQuery { get; }
        public readonly List<Tuple<string, SqlTable>> EmbeddedTables;
        public bool IsEmbedded;

        private readonly string _patchScript;
        private readonly IDbConnection _connection;
        private SqlReader _reader;
        private JsPatch _patcher;

        public SqlTable(string tableName, string query, string patch, IDbConnection connection)
        {
            Name = tableName;
            PrimaryKeys = new List<string>();
            ForeignKeys = new Dictionary<string, string>();
            InitialQuery = string.IsNullOrEmpty(query) ? Queries.SelectTable(Name) : query;
            EmbeddedTables = new List<Tuple<string, SqlTable>>();
            IsEmbedded = false;

            _patchScript = string.IsNullOrEmpty(patch) ? null : patch;
            _connection = connection;
        }

        public void Embed(SqlTable table, string property = null)
        {
            if (string.IsNullOrEmpty(property))
                property = table.Name;

            EmbeddedTables.Add(Tuple.Create(property, table));
            table.IsEmbedded = true;
        }

        public JsPatch GetJsPatcher()
        {
            if (_patcher == null && !string.IsNullOrEmpty(_patchScript))
                _patcher = new JsPatch(_patchScript);

            return _patcher;
        }

        public List<string> GetColumnsReferencingTable(string parentTableName)
        {
            var lst = new List<string>();

            foreach (var item in ForeignKeys)
                if (item.Value == parentTableName)
                    lst.Add(item.Key);

            return lst;
        }

        public SqlReader GetReader(List<string> columns = null)
        {
            if (_reader != null) return _reader;

            var query = InitialQuery + Queries.OrderByColumns(columns ?? PrimaryKeys);
            _reader = new SqlReader(_connection, query, columns != null);

            return _reader;
        }

        public SqlReader GetReaderWhere(List<string> columns, List<string> values)
        {
            var query = Queries.SelectFromQueryWhere(InitialQuery, columns, values) + Queries.OrderByColumns(columns);

            if (_reader != null)
            {
                _reader.SetCommand(query);
                return _reader;
            }

            _reader = new SqlReader(_connection, query, columns != null);
            return _reader;
        }
    }
}
