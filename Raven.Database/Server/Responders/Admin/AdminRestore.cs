using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
	class AdminRestore : AdminResponder
	{
		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

			var restoreRequest = context.ReadJsonObject<RestoreRequest>();

			var ravenConfiguration = new RavenConfiguration();
			if (File.Exists(Path.Combine(restoreRequest.RestoreLocation, "Raven.ravendb")))
			{
				ravenConfiguration.DefaultStorageTypeName = "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
			}
			else if (Directory.Exists(Path.Combine(restoreRequest.RestoreLocation, "new")))
			{
				ravenConfiguration.DefaultStorageTypeName = "Raven.Storage.Esent.TransactionalStorage, Raven.Storage.Esent";
			}
			var restoreDoc = new List<string>();

			// Commented out unfinished code to get it to compile
			//		    DocumentDatabase.Restore(ravenConfiguration, restoreRequest.RestoreLocation, restoreRequest.DatabaseLocation,
			//		                             msg =>
			//		                             {
			//
			//		                             });
			//            SystemDatabase.
		}
	}
}
