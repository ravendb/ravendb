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
		}

		public PrincipalWithDatabaseAccess(WindowsPrincipal principal, List<string> adminDatabases)
			: this(principal)
		{
			AdminDatabases = adminDatabases;
		}

		public bool IsInRole(string role)
		{
			return Principal.IsInRole(role);
		}

		public IIdentity Identity { get; private set; }
		public List<string> AdminDatabases { get; set; }
		//TODO: to dictionaty of Admin, ReadOnly and Read/Write
	}
}