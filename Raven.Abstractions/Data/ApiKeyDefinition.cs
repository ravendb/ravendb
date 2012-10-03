using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class ApiKeyDefinition
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public string Secret { get; set; }
		public bool Enabled { get; set; }

		public List<DatabaseAccess> Databases { get; set; }

	    public ApiKeyDefinition()
	    {
	        Databases = new List<DatabaseAccess>();
	    }
	}

	public class DatabaseAccess
	{
		public bool Admin { get; set; }
		public string TenantId { get; set; }
		public bool ReadOnly { get; set; }
	}
}