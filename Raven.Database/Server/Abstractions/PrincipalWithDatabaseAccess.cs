using System.Collections.Generic;
using System.Security.Principal;

namespace Raven.Database.Server.Abstractions
{
	class PrincipalWithDatabaseAccess : IPrincipal
	{
		public readonly WindowsPrincipal Principal;

		public PrincipalWithDatabaseAccess(WindowsPrincipal principal)
		{
			Principal = principal;
			Identity = principal.Identity;
            AdminDatabases = new List<string>();
            ReadOnlyDatabases = new List<string>();
            ReadWriteDatabases = new List<string>();
		}

		public bool IsInRole(string role)
		{
			return Principal.IsInRole(role);
		}

		public IIdentity Identity { get; private set; }
        public List<string> AdminDatabases { get; set; }
        public List<string> ReadOnlyDatabases { get; set; }
        public List<string> ReadWriteDatabases { get; set; }
	}
}