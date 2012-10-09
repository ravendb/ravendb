using System;
using System.Linq;
using Raven.Abstractions;

namespace Raven.Database.Server.Security.OAuth
{
	public class AccessTokenBody
	{
		public string UserId { get; set; }
		public DatabaseAccess[] AuthorizedDatabases { get; set; }
		public double Issued { get; set; }

		public bool IsExpired()
		{
			var issued = DateTime.MinValue.AddMilliseconds(Issued);
			return SystemTime.UtcNow.Subtract(issued).TotalMinutes > 30;
		}

		public bool IsAuthorized(string tenantId, bool writeAccess)
		{
			if (AuthorizedDatabases == null)
				return false;
			var db = AuthorizedDatabases.FirstOrDefault(a => 
				
					string.Equals(a.TenantId,tenantId, StringComparison.OrdinalIgnoreCase) || 
					string.Equals(a.TenantId, "*")

				);
			if (db == null)
				return false;

			if (writeAccess && db.ReadOnly)
				return false;

			return true;
		}

		public class DatabaseAccess
		{
			public bool Admin { get; set; }
			public string TenantId { get; set; }
			public bool ReadOnly { get; set; }
		}
	}

	
}