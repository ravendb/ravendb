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
	}
}