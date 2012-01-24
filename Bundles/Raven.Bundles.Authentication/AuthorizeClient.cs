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

			var jsonDocument = ((DocumentDatabase)currentStore).Get("Raven/Users/"+username, null);
			if (jsonDocument == null)
			{
				return false;
			}

			var user = jsonDocument.DataAsJson.JsonDeserialization<AuthenticationUser>();

			var validatePassword = user.ValidatePassword(password);
			if (validatePassword)
			{
				allowedDatabases = user.AllowedDatabases.Select(tenantId=> new AccessTokenBody.DatabaseAccess
				{
					TenantId = tenantId
				}).ToArray();
			}

			return validatePassword;
		}

	}
}