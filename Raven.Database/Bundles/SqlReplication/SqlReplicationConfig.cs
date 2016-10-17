using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;

namespace Raven.Database.Bundles.SqlReplication
{
    internal class SqlReplicationConfigWithLastReplicatedEtag : SqlReplicationConfig
    {
        public SqlReplicationConfigWithLastReplicatedEtag(SqlReplicationConfig config, Etag lastReplicatedEtag)
        {
            LastReplicatedEtag = lastReplicatedEtag;

            ConnectionString = config.ConnectionString;
            ConnectionStringName = config.ConnectionStringName;
            ConnectionStringSettingName = config.ConnectionStringSettingName;
            Disabled = config.Disabled;
            FactoryName = config.FactoryName;
            ForceSqlServerQueryRecompile = config.ForceSqlServerQueryRecompile;
            Id = config.Id;
            Name = config.Name;
            ParameterizeDeletesDisabled = config.ParameterizeDeletesDisabled;
            PredefinedConnectionStringSettingName = config.PredefinedConnectionStringSettingName;
            QuoteTables = config.QuoteTables;
            RavenEntityName = config.RavenEntityName;
            Script = config.Script;
            SqlReplicationTables = config.SqlReplicationTables;
        }

        public Etag LastReplicatedEtag { get; private set; }
    }

    public class SqlReplicationConfig
    {
        public SqlReplicationConfig()
        {
            SqlReplicationTables = new List<SqlReplicationTable>();
        }

        public string Id { get; set; }

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public bool ParameterizeDeletesDisabled { get; set; }
        public bool ForceSqlServerQueryRecompile { get; set; }

        [Obsolete("Use QuoteTables, instead", true)]
        public bool PerformTableQuatation {
            get { return QuoteTables; }
            set { QuoteTables = value; }
        } 
        public bool QuoteTables { get; set; }
        public string RavenEntityName { get; set; }
        public string Script { get; set; }
        public string FactoryName { get; set; }

        public string ConnectionString { get; set; }

        public string PredefinedConnectionStringSettingName { get; set; }
        public string ConnectionStringName { get; set; }
        public string ConnectionStringSettingName { get; set; }

        public int? CommandTimeout { get; set; }

        public List<SqlReplicationTable> SqlReplicationTables { get; set; }
    }

    public class SqlReplicationTable
    {
        public string TableName { get; set; }
        public string DocumentKeyColumn { get; set; }
        public bool InsertOnlyMode { get; set; }

        protected bool Equals(SqlReplicationTable other)
        {
            return string.Equals(TableName, other.TableName) && string.Equals(DocumentKeyColumn, other.DocumentKeyColumn);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SqlReplicationTable) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((TableName != null ? TableName.GetHashCode() : 0)*397) ^ (DocumentKeyColumn != null ? DocumentKeyColumn.GetHashCode() : 0);
            }
        }
    }

    public class SqlReplicationConnections<TClass> where TClass : SqlReplicationConnections.PredefinedSqlConnection
    {
        public List<TClass> PredefinedConnections { get; set; }

        public SqlReplicationConnections()
        {
            PredefinedConnections = new List<TClass>();
        }
    }

    public class SqlReplicationConnections : SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnection>
    {
        public class PredefinedSqlConnection
        {
            public string Name { get; set; }
            public string FactoryName { get; set; }
            public string ConnectionString { get; set; }
        }

        public class PredefinedSqlConnectionWithConfigurationOrigin : PredefinedSqlConnection
        {
            public bool HasGlobal { get; set; }

            public bool HasLocal { get; set; }
        }
    }
}
