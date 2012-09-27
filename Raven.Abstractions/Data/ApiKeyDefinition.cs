namespace Raven.Abstractions.Data
{
	public class ApiKeyDefinition
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string Secret { get; set; }
		public bool Enabled { get; set; }

		public DatabaseAccess[] Databases { get; set; }
	}

	public class DatabaseAccess
	{
		public bool Admin { get; set; }
		public string TenantId { get; set; }
		public bool ReadOnly { get; set; }
	}
}