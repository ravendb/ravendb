using System;
using System.Collections.Generic;
using System.Data;
using Raven.Server.Config;

namespace Raven.Server.SqlMigration
{
    public class SqlTable
    {
        public readonly string Name;
        public readonly List<string> PrimaryKeys;
        public Dictionary<string, string> ForeignKeys { get; private set; }
        public readonly string InitialQuery;
        public readonly List<SqlEmbeddedTable> EmbeddedTables;

        protected readonly string PatchScript;
        protected readonly IDbConnection Connection;
        protected readonly RavenConfiguration RavenConfiguration;
        protected JsPatch Patcher;
        protected SqlReader Reader;
        public bool IsEmbedded;

        public SqlTable(string tableName, string query, string patch, IDbConnection connection, RavenConfiguration config)
        {
            Name = tableName;
            PrimaryKeys = new List<string>();
            ForeignKeys = new Dictionary<string, string>();
            InitialQuery = string.IsNullOrEmpty(query) ? SqlQueries.SelectTable(Name) : query;
            EmbeddedTables = new List<SqlEmbeddedTable>();

            PatchScript = string.IsNullOrEmpty(patch) ? null : patch;
            Connection = connection;
            RavenConfiguration = config;

            IsEmbedded = false;
        }

        public JsPatch GetJsPatcher()
        {
            return Patcher ?? (Patcher = new JsPatch(PatchScript, RavenConfiguration));
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
