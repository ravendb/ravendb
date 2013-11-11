using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Principal;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Data;
using System.Net.Http;
using Raven.Json.Linq;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminController : BaseAdminController
	{
		[HttpPost]
		[Route("admin/backup")]
		[Route("databases/{databaseName}/admin/backup")]
		public async Task<HttpResponseMessage> Backup()
		{
			var backupRequest = await ReadJsonObjectAsync<BackupRequest>();
			var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
			bool incrementalBackup;
			if (bool.TryParse(incrementalString, out incrementalBackup) == false)
				incrementalBackup = false;
			Database.StartBackup(backupRequest.BackupLocation, incrementalBackup, backupRequest.DatabaseDocument);

			return GetEmptyMessage(HttpStatusCode.Created);		
		}

		protected WindowsBuiltInRole[] AdditionalSupportedRoles
		{
			get
			{
				return new[] { WindowsBuiltInRole.BackupOperator };
			}
		}

		[HttpPost]
		[Route("admin/restore")]
		[Route("databases/{databaseName}/admin/restore")]
		public async Task<HttpResponseMessage> Restore()
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("Restore is only possiable from the system database", HttpStatusCode.BadRequest);

			var restoreRequest = await ReadJsonObjectAsync<RestoreRequest>();

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
				return
					GetMessageWithString(
						"A database name must be supplied if the restore location does not contain a valid Database.Document file",
						HttpStatusCode.BadRequest);
			}

			if (databaseName == Constants.SystemDatabase)
				return GetMessageWithString("Cannot do an online restore for the <system> database", HttpStatusCode.BadRequest);

			var ravenConfiguration = new RavenConfiguration
			{
				DatabaseName = databaseName,
				IsTenantDatabase = true
			};

			if (databaseDocument != null)
			{
				foreach (var setting in databaseDocument.Settings)
				{
					ravenConfiguration.Settings[setting.Key] = setting.Value;
				}
			}

			if (File.Exists(Path.Combine(restoreRequest.RestoreLocation, "Raven.ravendb")))
				ravenConfiguration.DefaultStorageTypeName = typeof (Raven.Storage.Managed.TransactionalStorage).AssemblyQualifiedName;
			else if (Directory.Exists(Path.Combine(restoreRequest.RestoreLocation, "new")))
				ravenConfiguration.DefaultStorageTypeName = typeof (Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

			ravenConfiguration.CustomizeValuesForTenant(databaseName);
			ravenConfiguration.Initialize();

			string documentDataDir;
			ravenConfiguration.DataDirectory = ResolveTenantDataDirectory(restoreRequest.DatabaseLocation, databaseName, out documentDataDir);

			var restoreStatus = new List<string>();
			DatabasesLandlord.SystemDatabase.Delete(RestoreStatus.RavenRestoreStatusDocumentKey, null, null);
			var defrag = "true".Equals(GetQueryStringValue("defrag"), StringComparison.InvariantCultureIgnoreCase);

			await Task.Factory.StartNew(() =>
			{
				DocumentDatabase.Restore(ravenConfiguration, restoreRequest.RestoreLocation, null,
					msg =>
					{
						restoreStatus.Add(msg);
						DatabasesLandlord.SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
							RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);
					}, defrag);

				if (databaseDocument == null)
					return;

				databaseDocument.Settings[Constants.RavenDataDir] = documentDataDir;
				databaseDocument.Id = databaseName;
				DatabasesLandlord.Protect(databaseDocument);
				DatabasesLandlord.SystemDatabase.Put("Raven/Databases/" + databaseName, null, RavenJObject.FromObject(databaseDocument),
					new RavenJObject(), null);

				restoreStatus.Add("The new database was created");
				DatabasesLandlord.SystemDatabase.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
					RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);
			}, TaskCreationOptions.LongRunning);

			return GetEmptyMessage();
		}

		private string ResolveTenantDataDirectory(string databaseLocation, string databaseName, out string documentDataDir)
		{
			if (Path.IsPathRooted(databaseLocation))
			{
				documentDataDir = databaseLocation;
				return databaseLocation;
			}

			var baseDataPath = Path.GetDirectoryName(DatabasesLandlord.SystemDatabase.Configuration.DataDirectory);
			if (baseDataPath == null)
				throw new InvalidOperationException("Could not find root data path");

			if (string.IsNullOrWhiteSpace(databaseLocation))
			{
				documentDataDir = Path.Combine("~/Databases", databaseName);
				return Path.Combine(baseDataPath, documentDataDir.Substring(2));
			}

			documentDataDir = databaseLocation;

			if (!documentDataDir.StartsWith("~/") && !documentDataDir.StartsWith(@"~\"))
				documentDataDir = "~/" + documentDataDir.TrimStart(new[] {'/', '\\'});

			return Path.Combine(baseDataPath, documentDataDir.Substring(2));
		}

		[HttpPost]
		[Route("admin/changedbid")]
		[Route("databases/{databaseName}/admin/changedbid")]
		public HttpResponseMessage ChangeDbId()
		{
			Guid old = Database.TransactionalStorage.Id;
			var newId = Database.TransactionalStorage.ChangeId();

			return GetMessageWithObject(new
			{
				OldId = old,
				NewId = newId
			});
		}

		[HttpPost]
		[Route("admin/compact")]
		[Route("databases/{databaseName}/admin/compact")]
		public HttpResponseMessage Compact()
		{
			EnsureSystemDatabase();
				
			var db = InnerRequest.RequestUri.ParseQueryString()["database"];
			if (string.IsNullOrWhiteSpace(db))
				return GetMessageWithString("Compact request requires a valid database parameter", HttpStatusCode.BadRequest);

			var configuration = DatabasesLandlord.CreateTenantConfiguration(db);
			if (configuration == null)
				return GetMessageWithString("No database named: " + db, HttpStatusCode.NotFound);

			DatabasesLandlord.LockDatabase(db, () => DatabasesLandlord.SystemDatabase.TransactionalStorage.Compact(configuration));

			return GetEmptyMessage();
		}

		[HttpGet]
		[Route("admin/indexingStatus")]
		[Route("databases/{databaseName}/admin/indexingStatus")]
		public HttpResponseMessage IndexingStatus()
		{
			return GetMessageWithObject(new {IndexingStatus = Database.WorkContext.RunIndexing ? "Indexing" : "Paused"});		
		}

		[HttpPost]
		[Route("admin/optimize")]
		[Route("databases/{databaseName}/admin/optimize")]
		public void Optimize()
		{
			Database.IndexStorage.MergeAllIndexes();			
		}

		[HttpPost]
		[Route("admin/startIndexing")]
		[Route("databases/{databaseName}/admin/startIndexing")]
		public void StartIndexing()
		{
			var concurrency = InnerRequest.RequestUri.ParseQueryString()["concurrency"];

			if (string.IsNullOrEmpty(concurrency) == false)
				Database.Configuration.MaxNumberOfParallelIndexTasks = Math.Max(1, int.Parse(concurrency));

			Database.SpinIndexingWorkers();
		}

		[HttpPost]
		[Route("admin/stopIndexing")]
		[Route("databases/{databaseName}/admin/stopIndexing")]
		public void StopIndexing()
		{
			Database.StopIndexingWorkers();			
		}

		[HttpGet]
		[Route("admin/stats")]
		[Route("databases/{databaseName}/admin/stats")]
		public HttpResponseMessage Stats()
		{
			if (Database != DatabasesLandlord.SystemDatabase)
				return GetMessageWithString("Admin stats can only be had from the root database", HttpStatusCode.NotFound);

			return GetMessageWithObject(DatabasesLandlord.SystemDatabase.Statistics);
		}

		[HttpGet]
		[Route("admin/gc")]
		[Route("databases/{databaseName}/admin/gc")]
		[HttpPost]
		[Route("admin/gc")]
		[Route("databases/{databaseName}/admin/gc")]
		public void Gc()
		{
			EnsureSystemDatabase();
			CollectGarbage(Database);
		}

		[HttpGet]
		[Route("admin/detailed-storage-breakdown")]
		[Route("databases/{databaseName}/admin/detailed-storage-breakdown")]
		public HttpResponseMessage DetailedStorageBreakdown()
		{
			var x = Database.TransactionalStorage.ComputeDetailedStorageInformation();
			return GetMessageWithObject(x);
		}

		[HttpGet]
		[Route("admin/loh-compaction")]
		[Route("databases/{databaseName}/admin/loh-compaction")]
		[HttpPost]
		[Route("admin/loh-compaction")]
		[Route("databases/{databaseName}/admin/loh-compaction")]		
		public void LohCompaction()
		{
			if (EnsureSystemDatabase() == false)
				return;

			RavenGC.CollectGarbage(true, () => Database.TransactionalStorage.ClearCaches());
		}

		public static void CollectGarbage(DocumentDatabase database)
		{
			GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
			database.TransactionalStorage.ClearCaches();
			GC.WaitForPendingFinalizers();
		}
	}
}