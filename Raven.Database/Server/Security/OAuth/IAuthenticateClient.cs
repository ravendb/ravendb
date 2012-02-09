using System.ComponentModel.Composition;

namespace Raven.Database.Server.Security.OAuth
{
	[InheritedExport]
	public interface IAuthenticateClient
	{
		bool Authenticate(DocumentDatabase currentDatabase, string username, string password, out AccessTokenBody.DatabaseAccess[] allowedDatabases);
	}
}