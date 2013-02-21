namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationConfig
	{
		public string Id { get; set; }

		public string Name { get; set; }

		public string RavenEntityName { get; set; }
		public string Script { get; set; }
		public string FactoryName { get; set; }

		public string ConnectionString { get; set; }

		public string ConnectionStringName { get; set; }
		public string ConnectionStringSettingName { get; set; }
	}
}