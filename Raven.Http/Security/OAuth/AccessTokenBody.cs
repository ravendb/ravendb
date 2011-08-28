using System;
using System.Linq;

namespace Raven.Http.Security.OAuth
{
	public class AccessTokenBody
	{
		public string UserId { get; set; }
		public string[] AuthorizedDatabases { get; set; }
		public double Issued { get; set; }

		public bool IsExpired()
		{
			var issued = DateTime.MinValue.AddMilliseconds(Issued);
			return !(issued < DateTime.UtcNow && DateTime.UtcNow.Subtract(issued) < TimeSpan.FromMinutes(30));
		}

		public bool IsAuthorized(string tenantId)
		{
			return AuthorizedDatabases != null && AuthorizedDatabases.Any(a => a.Equals(tenantId, StringComparison.OrdinalIgnoreCase) || a == "*");
		}
	}
}