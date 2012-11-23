using System;
using System.Collections.Generic;
using System.Security.Principal;

namespace Raven.Database.Server.Abstractions
{
	internal class PrincipalWithDatabaseAccess : IPrincipal
	{
		public readonly WindowsPrincipal Principal;

		public PrincipalWithDatabaseAccess(WindowsPrincipal principal)
		{
			Principal = principal;
			Identity = principal.Identity;
			AdminDatabases = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			ReadOnlyDatabases = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
			ReadWriteDatabases = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		}

		public bool IsInRole(string role)
		{
			return Principal.IsInRole(role);
		}

		public bool? ExplicitlyConfigured { get; set; }
		public IIdentity Identity { get; private set; }
		public HashSet<string> AdminDatabases { get; private set; }
		public HashSet<string> ReadOnlyDatabases { get; private set; }
		public HashSet<string> ReadWriteDatabases { get; private set; }
	}
}