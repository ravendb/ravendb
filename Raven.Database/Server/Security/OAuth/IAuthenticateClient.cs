using System.ComponentModel.Composition;

namespace Raven.Http.Security.OAuth
{
	[InheritedExport]
	public interface IAuthenticateClient
	{
		bool Authenticate(IResourceStore currentStore, string username, string password, out string[] allowedDatabases);
	}
}