using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Raven.Server.SqlMigration
{
    class SqlTable
    {
        private readonly SqlDatabase _database;

        public readonly string Name;
        public Dictionary<string, Tuple<string, string>> References;
        public List<string> PrimaryKeys;
        public bool IsEmbedded;
        public Dictionary<string, string> EmbeddedTables;
        public bool IsReferenced;

        public SqlTable(SqlDatabase database, string tableName)
        {
            _database = database;
            Name = tableName;
            References = new Dictionary<string, Tuple<string, string>>();
            PrimaryKeys = new List<string>();
            IsEmbedded = false;
            EmbeddedTables = new Dictionary<string, string>();
            IsReferenced = false;

            PrimaryKeys.Sort();
        }

        public void AddEmbeddedTable(string propertyName, string childTable)
        {
            EmbeddedTables.Add(propertyName, childTable);
        }

        public KeyValuePair<string, Tuple<string, string>> GetReferenceColumnNameByTableName(string name)
        {
            foreach (var r in References)
            {
                if (r.Value.Item1 == name)
                    return r;
            }

            throw new InvalidOperationException($"Reference with table name {name} was not found");
        }
    }
}
