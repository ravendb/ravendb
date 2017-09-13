using System.Collections.Generic;
using System.Data;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.SqlMigration
{
    public class SqlTable
    {
        public readonly string Name;
        public readonly List<string> PrimaryKeys;
        public Dictionary<string, string> ForeignKeys;
        public readonly string InitialQuery;
        public readonly List<SqlEmbeddedTable> EmbeddedTables;
        public bool IsEmbedded;

        protected JsPatch Patcher;
        protected readonly string PatchScript;
        protected readonly IDbConnection Connection;
        protected SqlReader Reader;

        public SqlTable(string tableName, string query, string patch, IDbConnection connection)
        {
            Name = tableName;
            PrimaryKeys = new List<string>();
            ForeignKeys = new Dictionary<string, string>();
            InitialQuery = string.IsNullOrEmpty(query) ? SqlQueries.SelectTable(Name) : query;
            EmbeddedTables = new List<SqlEmbeddedTable>();

            PatchScript = string.IsNullOrEmpty(patch) ? null : patch;
            Connection = connection;

            IsEmbedded = false;
        }

        public JsPatch GetJsPatch()
        {
            return Patcher ?? (Patcher = new JsPatch(PatchScript));
        }

        public SqlReader GetReader()
        {
            if (Reader != null) return Reader;

            var query = InitialQuery + SqlQueries.OrderByColumns(PrimaryKeys);
            Reader = new SqlReader(Connection, query);
            return Reader;
        }
    }
}
