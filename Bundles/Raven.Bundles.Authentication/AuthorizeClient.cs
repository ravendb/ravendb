using System.Collections.Generic;
using Raven.Database;
using Raven.Database.Server.Security.OAuth;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Bundles.Authentication
{
	public class AuthenticateClient : IAuthenticateClient
	{
		public bool Authenticate(DocumentDatabase currentStore, string username, string password, out AccessTokenBody.DatabaseAccess[] allowedDatabases)
		{
			allowedDatabases = new AccessTokenBody.DatabaseAccess[0];

			var jsonDocument = currentStore.Get("Raven/Users/" + username, null);
			if (jsonDocument == null)
			{
				return false;
			}
			var user = jsonDocument.DataAsJson.JsonDeserialization<AuthenticationUser>();

			var validatePassword = user.ValidatePassword(password);
			if (!validatePassword)
				return false;

			var dbs = Enumerable.Empty<AccessTokenBody.DatabaseAccess>();
			if (user.AllowedDatabases != null)
			{
				var accesses = user.AllowedDatabases.Select(tenantId => new AccessTokenBody.DatabaseAccess
				{
					TenantId = tenantId,
					Admin = user.Admin,
					ReadOnly = false
				});
				dbs = dbs.Concat(accesses);
			}

			if (user.Databases != null)
			{
				var accesses = user.Databases.Select(x => new AccessTokenBody.DatabaseAccess
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