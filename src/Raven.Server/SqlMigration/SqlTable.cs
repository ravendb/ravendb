using System.Collections.Generic;

namespace Raven.Server.SqlMigration
{
    public abstract class SqlTable
    {
        public readonly string Name;
        public readonly string NewName;
        public readonly SqlDatabase Database;
        public readonly List<string> PrimaryKeys;
        public readonly string InitialQuery;
        public readonly List<SqlEmbeddedTable> EmbeddedTables;
        public Dictionary<string, string> ForeignKeys;
        public bool IsEmbedded;
        public SqlReader Reader;

        protected SqlTable(string tableName, string query, SqlDatabase database, string newName = null)
        {
            Name = tableName;
            NewName = string.IsNullOrEmpty(newName) ? Name : newName;
            PrimaryKeys = new List<string>();
            ForeignKeys = new Dictionary<string, string>();
            InitialQuery = string.IsNullOrEmpty(query) ? SqlQueries.SelectTable(Name) : query;
            EmbeddedTables = new List<SqlEmbeddedTable>();
            Database = database;
        }

        public abstract SqlReader GetReader();
    }
}
