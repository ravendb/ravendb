using System.ComponentModel.Composition;
using Raven.Database;

namespace Raven.Http.Security.OAuth
{
	[InheritedExport]
	public interface IAuthenticateClient
	{
		bool Authenticate(DocumentDatabase currentDatabase, string username, string password, out string[] allowedDatabases);
	}
}