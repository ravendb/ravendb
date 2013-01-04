using System.Collections.Generic;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Security.OAuth
{
	[InheritedExport]
	public interface IAuthenticateClient
	{
		bool Authenticate(DocumentDatabase currentDatabase, string username, string password, out List<DatabaseAccess> allowedDatabases);
	}
}