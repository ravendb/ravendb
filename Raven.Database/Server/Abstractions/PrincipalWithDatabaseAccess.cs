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
			AdminDatabases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			ReadOnlyDatabases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
			ReadWriteDatabases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            ReadOnlyFileSystems = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            ReadWriteFileSystems = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		}

		public bool IsInRole(string role)
		{
			return Principal.IsInRole(role);
		}

		public IIdentity Identity { get; private set; }
		public HashSet<string> AdminDatabases { get; private set; }
		public HashSet<string> ReadOnlyDatabases { get; private set; }
		public HashSet<string> ReadWriteDatabases { get; private set; }
        public HashSet<string> ReadOnlyFileSystems { get; private set; }
        public HashSet<string> ReadWriteFileSystems{ get; private set; }
	}
}