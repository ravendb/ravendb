using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Security.OAuth
{
	public class AccessTokenBody
	{
		public string UserId { get; set; }
		public List<DatabaseAccess> AuthorizedDatabases { get; set; }
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
			DatabaseAccess db;
			if (string.Equals(tenantId, "<system>"))
			{
				db = AuthorizedDatabases.FirstOrDefault(access => string.Equals(access.TenantId, "<system>"));
			}
			else
			{
				db = AuthorizedDatabases.FirstOrDefault(a =>
				                                        string.Equals(a.TenantId, tenantId, StringComparison.OrdinalIgnoreCase) ||
				                                        string.Equals(a.TenantId, "*"));
			}

			if (db == null)
				return false;

			if (writeAccess && db.ReadOnly)
				return false;

			return true;
		}
	}

	
}