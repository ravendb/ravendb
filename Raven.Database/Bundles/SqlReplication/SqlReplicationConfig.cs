using System.Collections.Generic;

namespace Raven.Database.Bundles.SqlReplication
{
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

		public string RavenEntityName { get; set; }
		public string Script { get; set; }
		public string FactoryName { get; set; }

		public string ConnectionString { get; set; }

		public string ConnectionStringName { get; set; }
		public string ConnectionStringSettingName { get; set; }

		public List<SqlReplicationTable> SqlReplicationTables { get; set; }
	}

	public class SqlReplicationTable
	{
		public string TableName { get; set; }
		public string DocumentKeyColumn { get; set; }

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
}