using System.Collections.Generic;
using System;
using System.IO;
using System.Security.Principal;
using System.Threading.Tasks;
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
		protected override WindowsBuiltInRole[] AdditionalSupportedRoles
		{
			get
			{
				return new[] { WindowsBuiltInRole.BackupOperator };
			}
		}

		public override void RespondToAdmin(IHttpContext context)
		{
			if (EnsureSystemDatabase(context) == false)
				return;

			var restoreRequest = context.ReadJsonObject<RestoreRequest>();
			var restoreStatus = new List<string>();
			SystemDatabase.Delete(RestoreStatus.RavenRestoreStatusDocumentKey, null, null);

			DatabaseDocument databaseDocument = null;

			if (File.Exists(Path.Combine(restoreRequest.RestoreLocation, "Database.Document")))
			{
				var databaseDocumentText = File.ReadAllText(Path.Combine(restoreRequest.RestoreLocation, "Database.Document"));
				databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
			}

			var databaseName = !string.IsNullOrWhiteSpace(restoreRequest.DatabaseName) ? restoreRequest.DatabaseName
								   : databaseDocument == null ? null : databaseDocument.Id;

			if (string.IsNullOrWhiteSpace(databaseName))
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "A database name must be supplied if the restore location does not contain a valid Database.Document file"
				});

				restoreStatus.Add("A database name must be supplied if the restore location does not contain a valid Database.Document file");
				SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
							   RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);

				return;
			}

            if (databaseName == Constants.SystemDatabase)
            {
                context.SetStatusToBadRequest();
                context.WriteJson(new
                {
                    Error = "Cannot do an online restore for the <system> database"
                });

				restoreStatus.Add("Cannot do an online restore for the <system> database");
				SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
							   RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);
                return;
            }


			var ravenConfiguration = new RavenConfiguration
			{
				DatabaseName = databaseName,
				IsTenantDatabase = true
			};

			if(databaseDocument != null)
			{
				foreach (var setting in databaseDocument.Settings)
				{
					ravenConfiguration.Settings[setting.Key] = setting.Value;
				}
			}

			if (File.Exists(Path.Combine(restoreRequest.RestoreLocation, "Raven.ravendb")))
			{
				ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName;

			}
			else if (Directory.Exists(Path.Combine(restoreRequest.RestoreLocation, "new")))
			{
				ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;
			}

			ravenConfiguration.CustomizeValuesForTenant(databaseName);
			ravenConfiguration.Initialize();

			string documentDataDir;
			ravenConfiguration.DataDirectory = ResolveTenantDataDirectory(restoreRequest.DatabaseLocation, databaseName, out documentDataDir);

			
			var defrag = "true".Equals(context.Request.QueryString["defrag"], StringComparison.InvariantCultureIgnoreCase);

			Task.Factory.StartNew(() =>
			{
				DocumentDatabase.Restore(ravenConfiguration, restoreRequest.RestoreLocation, null,
				                         msg =>
				                         {
					                         restoreStatus.Add(msg);
					                         SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
					                                            RavenJObject.FromObject(new {restoreStatus}), new RavenJObject(), null);
				                         }, defrag);

				if (databaseDocument == null)
				{
					restoreStatus.Add("Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name");
					SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
								   RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);
					return;
				}
					
				databaseDocument.Settings[Constants.RavenDataDir] = documentDataDir;
				databaseDocument.Id = databaseName;
				server.Protect(databaseDocument);
				SystemDatabase.Put("Raven/Databases/" + databaseName, null, RavenJObject.FromObject(databaseDocument),
				                   new RavenJObject(), null);

				restoreStatus.Add("The new database was created");
				SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
				                   RavenJObject.FromObject(new {restoreStatus}), new RavenJObject(), null);
			}, TaskCreationOptions.LongRunning);
		}

		private string ResolveTenantDataDirectory(string databaseLocation, string databaseName, out string documentDataDir)
		{
			if (Path.IsPathRooted(databaseLocation))
			{
				documentDataDir = databaseLocation;
				return databaseLocation;
			}

			var baseDataPath = Path.GetDirectoryName(SystemDatabase.Configuration.DataDirectory);
			if (baseDataPath == null)
				throw new InvalidOperationException("Could not find root data path");

			if (string.IsNullOrWhiteSpace(databaseLocation))
			{
				documentDataDir = Path.Combine("~/Databases", databaseName);
				return Path.Combine(baseDataPath, documentDataDir.Substring(2));
			}

			documentDataDir = databaseLocation;

			if (!documentDataDir.StartsWith("~/") && !documentDataDir.StartsWith(@"~\"))
			{
				documentDataDir = "~/" + documentDataDir.TrimStart(new char[] { '/', '\\' });
			}

			return Path.Combine(baseDataPath, documentDataDir.Substring(2));
		}
	}
}