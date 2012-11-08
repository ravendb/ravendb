using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

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

			var restoreStatus = new List<string>();

			DocumentDatabase.Restore(ravenConfiguration, restoreRequest.RestoreLocation, restoreRequest.DatabaseLocation,
			                         msg =>
			                         {
				                         restoreStatus.Add(msg);
				                         SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
											 RavenJObject.FromObject(new {restoreStatus}), new RavenJObject(), null);
			                         });

			if (File.Exists(Path.Combine(restoreRequest.RestoreLocation, "Database.Document")))
			{
				var databaseDocumentText = File.ReadAllText(Path.Combine(restoreRequest.RestoreLocation, "Database.Document"));
				var databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
				if (databaseDocument == null)
					return;

				if (!string.IsNullOrWhiteSpace(restoreRequest.DatabaseLocation))
					databaseDocument.Settings[Constants.RavenDataDir] = restoreRequest.DatabaseLocation;
				if (!string.IsNullOrWhiteSpace(restoreRequest.DatabaseName))
					databaseDocument.Id = "Raven/Databases/" + restoreRequest.DatabaseName;
				SystemDatabase.Put(databaseDocument.Id, null, RavenJObject.FromObject(databaseDocument), new RavenJObject(), null);

				restoreStatus.Add("The new database was created");
				SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
					RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);
			}
		}
	}
}