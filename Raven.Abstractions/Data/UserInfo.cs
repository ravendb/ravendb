using System.Collections.Generic;
using System.Security.Principal;
using Raven.Database.Server.Security.OAuth;

namespace Raven.Abstractions.Data
{
	public class UserInfo
	{
		public string Remark { get; set; }
		public string User { get; set; }
		public bool IsAdminGlobal { get; set; }
		public bool IsAdminCurrentDb { get; set; }
		public List<DatabaseInfo> Databases { get; set; }
		public IPrincipal Principal { get; set; }
		public HashSet<string> AdminDatabases { get; set; }
		public HashSet<string> ReadOnlyDatabases { get; set; }
		public HashSet<string> ReadWriteDatabases { get; set; }
		public AccessTokenBody AccessTokenBody { get; set; }
	}

	public class DatabaseInfo
	{
		public string Database { get; set; }
		public bool IsAdmin { get; set; }
	}
}
