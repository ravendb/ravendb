using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationConfiguration
    {
        public SqlReplicationConfiguration()
        {
            SqlReplicationTables = new List<SqlReplicationTable>();
        }

        public string Id { get; set; }

        public string Name { get; set; }

        public bool Disabled { get; set; }

        public bool ParameterizeDeletesDisabled { get; set; }

        public bool ForceSqlServerQueryRecompile { get; set; }

        public bool QuoteTables { get; set; }

        public string Collection { get; set; }

        public string Script { get; set; }

        public string ConnectionStringName { get; set; }

        public List<SqlReplicationTable> SqlReplicationTables { get; set; }
    }

    public class SqlReplicationTable
    {
        public string TableName { get; set; }
        public string DocumentKeyColumn { get; set; }
        public bool InsertOnlyMode { get; set; }

        protected bool Equals(SqlReplicationTable other)
        {
            return string.Equals(TableName, other.TableName) && 
                string.Equals(DocumentKeyColumn, other.DocumentKeyColumn);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SqlReplicationTable)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((TableName?.GetHashCode() ?? 0)*397) ^
                       (DocumentKeyColumn?.GetHashCode() ?? 0);
            }
        }
    }
}