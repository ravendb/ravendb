using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Server.Security.OAuth;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Bundles.Authentication
{
	public class AuthenticateClient : IAuthenticateClient
	{
		public bool Authenticate(DocumentDatabase currentStore, string username, string password, out DatabaseAccess[] allowedDatabases)
		{
			allowedDatabases = new DatabaseAccess[0];

			var jsonDocument = currentStore.Get("Raven/Users/" + username, null);
			if (jsonDocument == null)
			{
				return false;
			}
			var user = jsonDocument.DataAsJson.JsonDeserialization<AuthenticationUser>();

			var validatePassword = user.ValidatePassword(password);
			if (!validatePassword)
				return false;

			var dbs = Enumerable.Empty<DatabaseAccess>();
			if (user.AllowedDatabases != null)
			{
				var accesses = user.AllowedDatabases.Select(tenantId => new DatabaseAccess
				{
					TenantId = tenantId,
					Admin = user.Admin,
					ReadOnly = false
				});
				dbs = dbs.Concat(accesses);
			}

			if (user.Databases != null)
			{
				var accesses = user.Databases.Select(x => new DatabaseAccess
				{
					Admin = user.Admin | x.Admin,
					ReadOnly = x.ReadOnly,
					TenantId = x.Name
				});
				dbs = dbs.Concat(accesses);
			}

			allowedDatabases = dbs.ToArray();

			return true;
		}

	}
}