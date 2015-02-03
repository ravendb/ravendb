using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Database.Actions;
using Raven.Database.Backup;
using Raven.Database.Config;
using System.Net.Http;
using Raven.Database.DiskIO;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Plugins.Builtins;
using Raven.Database.Server.Connections;
using Raven.Database.FileSystem;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Storage;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using Voron.Impl.Backup;

using Raven.Client.Extensions;

using MaintenanceActions = Raven.Database.Actions.MaintenanceActions;

namespace Raven.Database.Server.Controllers.Admin
{
	[RoutePrefix("")]
	public class AdminController : BaseAdminController
	{
		private static readonly HashSet<string> TasksToFilterOut = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
		                                                           {
			                                                          typeof(AuthenticationForCommercialUseOnly).FullName,
																	  typeof(RemoveBackupDocumentStartupTask).FullName,
																	  typeof(CreateFolderIcon).FullName,
																	  typeof(DeleteRemovedIndexes).FullName
		                                                           };

		[HttpPost]
		[RavenRoute("admin/backup")]
		[RavenRoute("databases/{databaseName}/admin/backup")]
		public async Task<HttpResponseMessage> Backup()
		{
			var backupRequest = await ReadJsonObjectAsync<DatabaseBackupRequest>();
			var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
			bool incrementalBackup;
			if (bool.TryParse(incrementalString, out incrementalBackup) == false)
				incrementalBackup = false;

			if (backupRequest.DatabaseDocument == null)
			{
				if (Database.Name == null || Database.Name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
				{
					backupRequest.DatabaseDocument = new DatabaseDocument { Id = Constants.SystemDatabase };
				}
				else
				{
					var jsonDocument = DatabasesLandlord.SystemDatabase.Documents.Get("Raven/Databases/" + Database.Name, null);
					if (jsonDocument != null)
					{
						backupRequest.DatabaseDocument = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
						DatabasesLandlord.Unprotect(backupRequest.DatabaseDocument);
						backupRequest.DatabaseDocument.Id = Database.Name;
					}
				}
			}

			Database.Maintenance.StartBackup(backupRequest.BackupLocation, incrementalBackup, backupRequest.DatabaseDocument);

			return GetEmptyMessage(HttpStatusCode.Created);
		}

		protected override WindowsBuiltInRole[] AdditionalSupportedRoles
		{
			get
			{
				return new[] { WindowsBuiltInRole.BackupOperator };
			}
		}

		[HttpPost]
		[RavenRoute("admin/restore")]
		[RavenRoute("databases/{databaseName}/admin/restore")]
		public async Task<HttpResponseMessage> Restore()
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("Restore is only possible from the system database", HttpStatusCode.BadRequest);

			var restoreStatus = new RestoreStatus { State = RestoreStatusState.Running, Messages = new List<string>() };

			var restoreRequest = await ReadJsonObjectAsync<DatabaseRestoreRequest>();

			DatabaseDocument databaseDocument = null;

			var databaseDocumentPath = MaintenanceActions.FindDatabaseDocument(restoreRequest.BackupLocation);
			if (File.Exists(databaseDocumentPath))
			{
				var databaseDocumentText = File.ReadAllText(databaseDocumentPath);
				databaseDocument = RavenJObject.Parse(databaseDocumentText).JsonDeserialization<DatabaseDocument>();
			}

			var databaseName = !string.IsNullOrWhiteSpace(restoreRequest.DatabaseName) ? restoreRequest.DatabaseName
								   : databaseDocument == null ? null : databaseDocument.Id;

			if (string.IsNullOrWhiteSpace(databaseName))
			{
				var errorMessage = (databaseDocument == null || String.IsNullOrWhiteSpace(databaseDocument.Id))
								? "Database.Document file is invalid - database name was not found and not supplied in the request (Id property is missing or null). This is probably a bug - should never happen."
								: "A database name must be supplied if the restore location does not contain a valid Database.Document file";

				restoreStatus.Messages.Add(errorMessage);
				DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null, RavenJObject.FromObject(new { restoreStatus }), new RavenJObject(), null);

				return GetMessageWithString(errorMessage, HttpStatusCode.BadRequest);
			}

			if (databaseName == Constants.SystemDatabase)
				return GetMessageWithString("Cannot do an online restore for the <system> database", HttpStatusCode.BadRequest);

			var existingDatabase = Database.Documents.GetDocumentMetadata("Raven/Databases/" + databaseName, null);
			if (existingDatabase != null)
				return GetMessageWithString("Cannot do an online restore for an existing database, delete the database " + databaseName + " and restore again.", HttpStatusCode.BadRequest);

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

			if (File.Exists(Path.Combine(restoreRequest.BackupLocation, BackupMethods.Filename)))
				ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
			else if (Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")))
				ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

			ravenConfiguration.CustomizeValuesForDatabaseTenant(databaseName);
			ravenConfiguration.Initialize();

			string documentDataDir;
			ravenConfiguration.DataDirectory = ResolveTenantDataDirectory(restoreRequest.DatabaseLocation, databaseName, out documentDataDir);
			restoreRequest.DatabaseLocation = ravenConfiguration.DataDirectory;

			string anotherRestoreResourceName;
			if (IsAnotherRestoreInProgress(out anotherRestoreResourceName))
			{
				if (restoreRequest.RestoreStartTimeout.HasValue)
				{
					try
					{
						using (var cts = new CancellationTokenSource())
						{
							cts.CancelAfter(TimeSpan.FromSeconds(restoreRequest.RestoreStartTimeout.Value));
							var token = cts.Token;
							do
							{
								await Task.Delay(500, token);
							}
							while (IsAnotherRestoreInProgress(out anotherRestoreResourceName));
						}
					}
					catch (OperationCanceledException)
					{
						return GetMessageWithString(string.Format("Another restore is still in progress (resource name = {0}). Waited {1} seconds for other restore to complete.", anotherRestoreResourceName, restoreRequest.RestoreStartTimeout.Value), HttpStatusCode.ServiceUnavailable);
					}
				}
				else
				{
					return GetMessageWithString(string.Format("Another restore is in progress (resource name = {0})", anotherRestoreResourceName), HttpStatusCode.ServiceUnavailable);
				}
			}
			Database.Documents.Put(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, RavenJObject.FromObject(new RestoreInProgress
																												{
																													Resource = databaseName
																												}), new RavenJObject(), null);

			DatabasesLandlord.SystemDatabase.Documents.Delete(RestoreStatus.RavenRestoreStatusDocumentKey, null, null);

			bool defrag;
			if (bool.TryParse(GetQueryStringValue("defrag"), out defrag))
				restoreRequest.Defrag = defrag;

			var task = Task.Factory.StartNew(() =>
			{
                try
                {
				    MaintenanceActions.Restore(ravenConfiguration, restoreRequest,
					    msg =>
					    {
						    restoreStatus.Messages.Add(msg);
						    DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
							    RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
					    });

				    if (databaseDocument == null)
					    return;

				    databaseDocument.Settings[Constants.RavenDataDir] = documentDataDir;
				    if (restoreRequest.IndexesLocation != null)
					    databaseDocument.Settings[Constants.RavenIndexPath] = restoreRequest.IndexesLocation;
				    if (restoreRequest.JournalsLocation != null)
					    databaseDocument.Settings[Constants.RavenTxJournalPath] = restoreRequest.JournalsLocation;

				    bool replicationBundleRemoved = false;
				    if (restoreRequest.DisableReplicationDestinations)
					    replicationBundleRemoved = TryRemoveReplicationBundle(databaseDocument);

				    databaseDocument.Id = databaseName;
				    DatabasesLandlord.Protect(databaseDocument);
				    DatabasesLandlord
					    .SystemDatabase
					    .Documents
					    .Put("Raven/Databases/" + databaseName, null, RavenJObject.FromObject(databaseDocument), new RavenJObject(), null);

				    restoreStatus.Messages.Add("The new database was created");
                    restoreStatus.State = RestoreStatusState.Completed;
				    DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
					    RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);

				    if (restoreRequest.GenerateNewDatabaseId) 
					    GenerateNewDatabaseId(databaseName);

				    if (replicationBundleRemoved)
					    AddReplicationBundleAndDisableReplicationDestinations(databaseName);

                }
                catch (Exception e)
                {
                    restoreStatus.State = RestoreStatusState.Faulted;
                    restoreStatus.Messages.Add("Unable to restore database " + e.Message);
                    DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
                               RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
                    throw;
                }
                finally
                {
                    Database.Documents.Delete(RestoreInProgress.RavenRestoreInProgressDocumentKey, null, null);
                }
			}, TaskCreationOptions.LongRunning);

			long id;
			Database.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
			{
				StartTime = SystemTime.UtcNow,
				TaskType = TaskActions.PendingTaskType.RestoreDatabase,
				Payload = "Restoring database " + databaseName + " from " + restoreRequest.BackupLocation
			}, out id);


			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

		private void GenerateNewDatabaseId(string databaseName)
		{
			Task<DocumentDatabase> databaseTask;
			if (DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, out databaseTask) == false)
				return;

			var database = databaseTask.Result;
			database.TransactionalStorage.ChangeId();
		}

		private static bool TryRemoveReplicationBundle(DatabaseDocument databaseDocument)
		{
			string value;
			if (databaseDocument.Settings.TryGetValue(Constants.ActiveBundles, out value) == false)
				return false;

			var bundles = value.GetSemicolonSeparatedValues();
			var removed = bundles.RemoveAll(n => n.Equals("Replication", StringComparison.OrdinalIgnoreCase)) > 0;

			databaseDocument.Settings[Constants.ActiveBundles] = string.Join(";", bundles);
			return removed;
		}

		private void AddReplicationBundleAndDisableReplicationDestinations(string databaseName)
		{
			Task<DocumentDatabase> databaseTask;
			if (DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, out databaseTask) == false)
				return;

			var database = databaseTask.Result;
			var configurationDocument = database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
			if (configurationDocument != null)
			{
				var replicationDocument = configurationDocument.MergedDocument;
				foreach (var destination in replicationDocument.Destinations)
				{
					destination.Disabled = true;
				}

				database
					.Documents
					.Put(Constants.RavenReplicationDestinations, null, RavenJObject.FromObject(replicationDocument), new RavenJObject(), null);
			}

			var databaseDocumentAsJson = DatabasesLandlord.SystemDatabase.Documents.Get(Constants.RavenDatabasesPrefix + databaseName, null);
			var databaseDocument = databaseDocumentAsJson.DataAsJson.JsonDeserialization<DatabaseDocument>();

			var bundles = databaseDocument.Settings[Constants.ActiveBundles].GetSemicolonSeparatedValues();
			bundles.Add("Replication");

			databaseDocument.Settings[Constants.ActiveBundles] = string.Join(";", bundles);

			DatabasesLandlord
					.SystemDatabase
					.Documents
					.Put(
						Constants.RavenDatabasesPrefix + databaseName,
						null,
						RavenJObject.FromObject(databaseDocument),
						new RavenJObject
					    {
						    { "Raven-Temp-Allow-Bundles-Change", true }
					    },
						null);
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
				documentDataDir = Path.Combine("~/", databaseName);
				return documentDataDir.ToFullPath(baseDataPath);
			}

			documentDataDir = databaseLocation;

			if (!documentDataDir.StartsWith("~/") && !documentDataDir.StartsWith(@"~\"))
			{
				documentDataDir = "~\\" + documentDataDir.TrimStart(new[] { '/', '\\' });
			}
			else if (documentDataDir.StartsWith("~/") || documentDataDir.StartsWith(@"~\"))
			{
				documentDataDir = "~\\" + documentDataDir.Substring(2);
			}

			return documentDataDir.ToFullPath(baseDataPath);
		}

		[HttpPost]
		[RavenRoute("admin/changedbid")]
		[RavenRoute("databases/{databaseName}/admin/changedbid")]
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
		[RavenRoute("admin/compact")]
		public HttpResponseMessage Compact()
		{
			var db = InnerRequest.RequestUri.ParseQueryString()["database"];
			if (string.IsNullOrWhiteSpace(db))
				return GetMessageWithString("Compact request requires a valid database parameter", HttpStatusCode.BadRequest);

			var configuration = DatabasesLandlord.CreateTenantConfiguration(db);
			if (configuration == null)
				return GetMessageWithString("No database named: " + db, HttpStatusCode.NotFound);

			var task = Task.Factory.StartNew(() =>
			{
                var compactStatus = new CompactStatus { State = CompactStatusState.Running, Messages = new List<string>() };
                DatabasesLandlord.SystemDatabase.Documents.Delete(CompactStatus.RavenDatabaseCompactStatusDocumentKey(db), null, null);

			    try
			    {

			        var targetDb = DatabasesLandlord.GetDatabaseInternal(db).ResultUnwrap();

			        DatabasesLandlord.Lock(db, () => targetDb.TransactionalStorage.Compact(configuration, msg =>
			        {
			            compactStatus.Messages.Add(msg);
			            DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenDatabaseCompactStatusDocumentKey(db), null,
			                                                           RavenJObject.FromObject(compactStatus), new RavenJObject(), null);

			        }));
                    compactStatus.State = CompactStatusState.Completed;
                    compactStatus.Messages.Add("Database compaction completed.");
                    DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenDatabaseCompactStatusDocumentKey(db), null,
                                                                       RavenJObject.FromObject(compactStatus), new RavenJObject(), null);
			    }
			    catch (Exception e)
			    {
                    compactStatus.Messages.Add("Unable to compact database " + e.Message);
                    compactStatus.State = CompactStatusState.Faulted;
                    DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenDatabaseCompactStatusDocumentKey(db), null,
                                                                       RavenJObject.FromObject(compactStatus), new RavenJObject(), null);
			        throw;
			    }
			    return GetEmptyMessage();
			});
			long id;
			Database.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
			{
				StartTime = SystemTime.UtcNow,
				TaskType = TaskActions.PendingTaskType.CompactDatabase,
				Payload = "Compact database " + db,
			}, out id);

			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

		[HttpGet]
		[RavenRoute("admin/indexingStatus")]
		[RavenRoute("databases/{databaseName}/admin/indexingStatus")]
		public HttpResponseMessage IndexingStatus()
		{
			string indexDisableStatus;
			bool result;
			if (bool.TryParse(Database.Configuration.Settings[Constants.IndexingDisabled], out result) && result)
			{
				indexDisableStatus = "Disabled";
			}
			else
			{
				indexDisableStatus = Database.WorkContext.RunIndexing ? "Indexing" : "Paused";
			}
			return GetMessageWithObject(new { IndexingStatus = indexDisableStatus });
		}

		[HttpPost]
		[RavenRoute("admin/optimize")]
		[RavenRoute("databases/{databaseName}/admin/optimize")]
		public void Optimize()
		{
			Database.IndexStorage.MergeAllIndexes();
		}

		[HttpPost]
		[RavenRoute("admin/startIndexing")]
		[RavenRoute("databases/{databaseName}/admin/startIndexing")]
		public void StartIndexing()
		{
			var concurrency = InnerRequest.RequestUri.ParseQueryString()["concurrency"];

			if (string.IsNullOrEmpty(concurrency) == false)
				Database.Configuration.MaxNumberOfParallelProcessingTasks = Math.Max(1, int.Parse(concurrency));

			Database.SpinBackgroundWorkers();
		}

		[HttpPost]
		[RavenRoute("admin/stopIndexing")]
		[RavenRoute("databases/{databaseName}/admin/stopIndexing")]
		public void StopIndexing()
		{
			Database.StopIndexingWorkers();
		}

		[HttpGet]
		[RavenRoute("admin/stats")]
		public HttpResponseMessage Stats()
		{
			if (Database != DatabasesLandlord.SystemDatabase)
				return GetMessageWithString("Admin stats can only be had from the root database", HttpStatusCode.NotFound);

			var stats = CreateAdminStats();
			return GetMessageWithObject(stats);
		}

		private AdminStatistics CreateAdminStats()
		{
			var allDbs = new List<DocumentDatabase>();
			var allFs = new List<RavenFileSystem>();
			DatabasesLandlord.ForAllDatabases(allDbs.Add);
			FileSystemsLandlord.ForAllFileSystems(allFs.Add);
			var currentConfiguration = DatabasesLandlord.SystemConfiguration;


			var stats = new AdminStatistics
			{
				ServerName = currentConfiguration.ServerName,
				TotalNumberOfRequests = RequestManager.NumberOfRequests,
				Uptime = SystemTime.UtcNow - RequestManager.StartUpTime,
				Memory = new AdminMemoryStatistics
				{
					DatabaseCacheSizeInMB = ConvertBytesToMBs(DatabasesLandlord.SystemDatabase.TransactionalStorage.GetDatabaseCacheSizeInBytes()),
					ManagedMemorySizeInMB = ConvertBytesToMBs(GetCurrentManagedMemorySize()),
					TotalProcessMemorySizeInMB = ConvertBytesToMBs(GetCurrentProcessPrivateMemorySize64()),
				},
				LoadedDatabases = LoadedDatabasesStats(allDbs),
				LoadedFileSystems = from fileSystem in allFs
									select fileSystem.GetFileSystemStats()
			};
			return stats;
		}

		private IEnumerable<LoadedDatabaseStatistics> LoadedDatabasesStats(IEnumerable<DocumentDatabase> allDbs)
		{
			foreach (var documentDatabase in allDbs)
			{
				LoadedDatabaseStatistics loadedDatabaseStatistics;
				try
				{
					var indexStorageSize = documentDatabase.GetIndexStorageSizeOnDisk();
					var transactionalStorageSize = documentDatabase.GetTransactionalStorageSizeOnDisk();
					var totalDatabaseSize = indexStorageSize + transactionalStorageSize.AllocatedSizeInBytes;
					var lastUsed = DatabasesLandlord.LastRecentlyUsed.GetOrDefault(documentDatabase.Name ?? Constants.SystemDatabase);
					loadedDatabaseStatistics = new LoadedDatabaseStatistics
					{
						Name = documentDatabase.Name,
						LastActivity = new[]
						{
							lastUsed,
							documentDatabase.WorkContext.LastWorkTime
						}.Max(),
						TransactionalStorageAllocatedSize = transactionalStorageSize.AllocatedSizeInBytes,
						TransactionalStorageAllocatedSizeHumaneSize = SizeHelper.Humane(transactionalStorageSize.AllocatedSizeInBytes),
						TransactionalStorageUsedSize = transactionalStorageSize.UsedSizeInBytes,
						TransactionalStorageUsedSizeHumaneSize = SizeHelper.Humane(transactionalStorageSize.UsedSizeInBytes),
						IndexStorageSize = indexStorageSize,
						IndexStorageHumaneSize = SizeHelper.Humane(indexStorageSize),
						TotalDatabaseSize = totalDatabaseSize,
						TotalDatabaseHumaneSize = SizeHelper.Humane(totalDatabaseSize),
						CountOfDocuments = documentDatabase.Statistics.CountOfDocuments,
						CountOfAttachments = documentDatabase.Statistics.CountOfAttachments,
						StorageStats = documentDatabase.TransactionalStorage.GetStorageStats(),
						DatabaseTransactionVersionSizeInMB = ConvertBytesToMBs(documentDatabase.TransactionalStorage.GetDatabaseTransactionVersionSizeInBytes()),
						Metrics = documentDatabase.CreateMetrics()
					};
				}
				catch (Exception e)
				{
					loadedDatabaseStatistics = new LoadedDatabaseStatistics
					{
						Name = documentDatabase.Name,
						TotalDatabaseHumaneSize = e.Message,
						IndexStorageHumaneSize = e.ToString()
					};
				}
				yield return loadedDatabaseStatistics;
			}
		}

		private decimal ConvertBytesToMBs(long bytes)
		{
			return Math.Round(bytes / 1024.0m / 1024.0m, 2);
		}

		private static long GetCurrentProcessPrivateMemorySize64()
		{
			using (var p = Process.GetCurrentProcess())
				return p.PrivateMemorySize64;
		}

		private static long GetCurrentManagedMemorySize()
		{
			var safelyGetPerformanceCounter = PerformanceCountersUtils.SafelyGetPerformanceCounter(
				".NET CLR Memory", "# Total committed Bytes", CurrentProcessName.Value);
			return safelyGetPerformanceCounter ?? GC.GetTotalMemory(false);
		}

		private static readonly Lazy<string> CurrentProcessName = new Lazy<string>(() =>
		{
			using (var p = Process.GetCurrentProcess())
				return p.ProcessName;
		});

		[HttpGet]
		[RavenRoute("admin/detailed-storage-breakdown")]
		[RavenRoute("databases/{databaseName}/admin/detailed-storage-breakdown")]
		public HttpResponseMessage DetailedStorageBreakdown()
		{
			var x = Database.TransactionalStorage.ComputeDetailedStorageInformation();
			return GetMessageWithObject(x);
		}

		[HttpPost]
		[HttpGet]
		[RavenRoute("admin/gc")]
		public HttpResponseMessage Gc()
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("Garbage Collection is only possible from the system database", HttpStatusCode.BadRequest);


			GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
			DatabasesLandlord.ForAllDatabases(documentDatabase => documentDatabase.TransactionalStorage.ClearCaches());
			GC.WaitForPendingFinalizers();

			return GetMessageWithString("GC Done");
		}

		[HttpGet]
		[HttpPost]
		[RavenRoute("admin/loh-compaction")]
		public HttpResponseMessage LohCompaction()
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("Large Object Heap Garbage Collection is only possible from the system database", HttpStatusCode.BadRequest);


			Action<DocumentDatabase> clearCaches = documentDatabase => documentDatabase.TransactionalStorage.ClearCaches();
			Action afterCollect = () => DatabasesLandlord.ForAllDatabases(clearCaches);

			RavenGC.CollectGarbage(true, afterCollect);

			return GetMessageWithString("LOH GC Done");
		}

		[HttpGet]
		[RavenRoute("admin/tasks")]
		[RavenRoute("databases/{databaseName}/admin/tasks")]
		public HttpResponseMessage Tasks()
		{
			return GetMessageWithObject(FilterOutTasks(Database.StartupTasks));
		}

		private static IEnumerable<string> FilterOutTasks(OrderedPartCollection<IStartupTask> tasks)
		{
			return tasks.Select(task => task.GetType().FullName).Where(t => !TasksToFilterOut.Contains(t)).ToList();
		}

		[HttpGet]
		[RavenRoute("admin/killQuery")]
		[RavenRoute("databases/{databaseName}/admin/killQuery")]
		public HttpResponseMessage KillQuery()
		{
			var idStr = GetQueryStringValue("id");
			long id;
			if (long.TryParse(idStr, out id) == false)
			{
				return GetMessageWithObject(new
				{
					Error = "Query string variable id must be a valid int64"
				}, HttpStatusCode.BadRequest);
			}

			var query = Database.WorkContext.CurrentlyRunningQueries
				.SelectMany(index => index.Value)
				.FirstOrDefault(q => q.QueryId == id);

			if (query != null)
			{
				query.TokenSource.Cancel();
			}

			return query == null ? GetEmptyMessage(HttpStatusCode.NotFound) : GetEmptyMessage(HttpStatusCode.NoContent);
		}

		[HttpGet]
		[RavenRoute("admin/debug/info-package")]
		public HttpResponseMessage InfoPackage()
		{
			var tempFileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
			try
			{
				var jsonSerializer = new JsonSerializer { Formatting = Formatting.Indented };
				jsonSerializer.Converters.Add(new EtagJsonConverter());

				using (var file = new FileStream(tempFileName, FileMode.Create))
				using (var package = new ZipArchive(file, ZipArchiveMode.Create))
				{
					var adminStats = package.CreateEntry("admin_stats.txt", CompressionLevel.Optimal);

					using (var metricsStream = adminStats.Open())
					using (var streamWriter = new StreamWriter(metricsStream))
					{
						jsonSerializer.Serialize(streamWriter, CreateAdminStats());
						streamWriter.Flush();
					}

					DatabasesLandlord.ForAllDatabases(database =>
					{
						var prefix = string.IsNullOrWhiteSpace(database.Name) ? "System" : database.Name;
						DebugInfoProvider.CreateInfoPackageForDatabase(package, database, RequestManager, prefix + "/");
					});

					var stacktraceRequsted = GetQueryStringValue("stacktrace");
					if (stacktraceRequsted != null)
					{
						DumpStacktrace(package);
					}
				}

				var response = new HttpResponseMessage();

				response.Content = new StreamContent(new FileStream(tempFileName, FileMode.Open, FileAccess.Read))
								   {
									   Headers =
									   {
										   ContentDisposition = new ContentDispositionHeaderValue("attachment")
																{
																	FileName = string.Format("Admin-Debug-Info-{0}.zip", SystemTime.UtcNow),
																},
										   ContentType = new MediaTypeHeaderValue("application/octet-stream")
									   }
								   };

				return response;
			}
			finally
			{
				IOExtensions.DeleteFile(tempFileName);
			}
		}

		private void DumpStacktrace(ZipArchive package)
		{
			var stacktraceRequsted = GetQueryStringValue("stacktrace");

			if (stacktraceRequsted != null)
			{
				var stacktrace = package.CreateEntry("stacktraces.txt", CompressionLevel.Optimal);

				var jsonSerializer = new JsonSerializer { Formatting = Formatting.Indented };
				jsonSerializer.Converters.Add(new EtagJsonConverter());

				using (var stacktraceStream = stacktrace.Open())
				{
					string ravenDebugDir = null;

					try
					{
						if (Debugger.IsAttached) throw new InvalidOperationException("Cannot get stacktraces when debugger is attached");

						ravenDebugDir = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
						var ravenDebugExe = Path.Combine(ravenDebugDir, "Raven.Debug.exe");
						var ravenDebugOutput = Path.Combine(ravenDebugDir, "stacktraces.txt");

						Directory.CreateDirectory(ravenDebugDir);

						if (Environment.Is64BitProcess) ExtractResource("Raven.Database.Util.Raven.Debug.x64.Raven.Debug.exe", ravenDebugExe);
						else ExtractResource("Raven.Database.Util.Raven.Debug.x86.Raven.Debug.exe", ravenDebugExe);

						var process = new Process { StartInfo = new ProcessStartInfo { Arguments = string.Format("-pid={0} /stacktrace -output={1}", Process.GetCurrentProcess().Id, ravenDebugOutput), FileName = ravenDebugExe, WindowStyle = ProcessWindowStyle.Hidden, } };

						process.Start();

						process.WaitForExit();

						if (process.ExitCode != 0)
							throw new InvalidOperationException("Raven.Debug exit code is: " + process.ExitCode);

						using (var stackDumpOutputStream = File.Open(ravenDebugOutput, FileMode.Open))
						{
							stackDumpOutputStream.CopyTo(stacktraceStream);
						}
					}
					catch (Exception ex)
					{
						var streamWriter = new StreamWriter(stacktraceStream);
						jsonSerializer.Serialize(streamWriter, new { Error = "Exception occurred during getting stacktraces of the RavenDB process. Exception: " + ex });
						streamWriter.Flush();
					}
					finally
					{
						if (ravenDebugDir != null && Directory.Exists(ravenDebugDir)) IOExtensions.DeleteDirectory(ravenDebugDir);
					}

					stacktraceStream.Flush();
				}
			}
		}

		private static void ExtractResource(string resource, string path)
		{
			var stream = typeof(DebugInfoProvider).Assembly.GetManifestResourceStream(resource);

			if (stream == null)
				throw new InvalidOperationException("Could not find the requested resource: " + resource);

			var bytes = new byte[4096];

			using (var stackDump = File.Create(path, 4096))
			{
				while (true)
				{
					var read = stream.Read(bytes, 0, bytes.Length);
					if (read == 0)
						break;

					stackDump.Write(bytes, 0, read);
				}

				stackDump.Flush();
			}
		}


		[HttpGet]
		[RavenRoute("admin/logs/configure")]
		public HttpResponseMessage OnAdminLogsConfig()
		{
			var id = GetQueryStringValue("id");
			if (string.IsNullOrEmpty(id))
			{
				return GetMessageWithObject(new
				{
					Error = "id query string parameter is mandatory when using logs endpoint"
				}, HttpStatusCode.BadRequest);
			}

			var logTarget = LogManager.GetTarget<AdminLogsTarget>();
			var connectionState = logTarget.For(id, this);

			var command = GetQueryStringValue("command");
			if (string.IsNullOrEmpty(command) == false)
			{
				if ("disconnect" == command)
				{
					logTarget.Disconnect(id);
				}
				return GetMessageWithObject(connectionState);
			}

			var watchCatogory = GetQueryStringValues("watch-category");
			var categoriesToWatch = watchCatogory.Select(
				x =>
				{
					var tokens = x.Split(':');
					LogLevel level;
					if (Enum.TryParse(tokens[1], out level))
					{
						return Tuple.Create(tokens[0], level);
					}
					throw new InvalidOperationException("Unable to parse watch-category: " + tokens[1]);
				}).ToList();

			var unwatchCatogory = GetQueryStringValues("unwatch-category");
			foreach (var category in unwatchCatogory)
			{
				connectionState.DisableLogging(category);
			}

			foreach (var categoryAndLevel in categoriesToWatch)
			{
				connectionState.EnableLogging(categoryAndLevel.Item1, categoryAndLevel.Item2);
			}

			return GetMessageWithObject(connectionState);

		}

		[HttpGet]
		[RavenRoute("admin/logs/events")]
		public HttpResponseMessage OnAdminLogsFetch()
		{
			var logsTransport = new LogsPushContent(this);
			logsTransport.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
			var logTarget = LogManager.GetTarget<AdminLogsTarget>();
			logTarget.Register(logsTransport);

			return new HttpResponseMessage { Content = logsTransport };
		}

		[HttpPost]
		[RavenRoute("databases/{databaseName}/admin/transactions/rollbackAll")]
		[RavenRoute("admin/transactions/rollbackAll")]
		public HttpResponseMessage Transactions()
		{
			var transactions = Database.TransactionalStorage.GetPreparedTransactions();
			foreach (var transactionContextData in transactions)
			{
				Database.Rollback(transactionContextData.Id);
			}

			return GetMessageWithObject(new { RolledBackTransactionsAmount = transactions.Count });
		}

		[HttpPost]
		[RavenRoute("admin/ioTest")]
		public async Task<HttpResponseMessage> IoTest()
		{
			if (EnsureSystemDatabase() == false)
			{
				return GetMessageWithString("IO Test is only possible from the system database", HttpStatusCode.BadRequest);
			}
		    var json = await ReadJsonAsync();
		    var testType = json.Value<string>("TestType");

		    AbstractPerformanceTestRequest ioTestRequest;
            switch (testType)
            {
                case GenericPerformanceTestRequest.Mode:
                    ioTestRequest = json.JsonDeserialization<GenericPerformanceTestRequest>();
                    break;
                case BatchPerformanceTestRequest.Mode:
                    ioTestRequest = json.JsonDeserialization<BatchPerformanceTestRequest>();
                    break;
                default: 
                    return GetMessageWithObject(new
                {
                    Error = "test type is invalid: " + testType
                }, HttpStatusCode.BadRequest);
            }

			if (Directory.Exists(ioTestRequest.Path) == false)
			{
				return GetMessageWithString(string.Format("Directory {0} doesn't exist.", ioTestRequest.Path), HttpStatusCode.BadRequest);
			}

            Database.Documents.Delete(AbstractDiskPerformanceTester.PerformanceResultDocumentKey, null, null);

			var killTaskCts = new CancellationTokenSource();

			var task = Task.Factory.StartNew(() =>
			{
				var debugInfo = new List<string>();

				using (var diskIo = AbstractDiskPerformanceTester.ForRequest(ioTestRequest, debugInfo.Add, killTaskCts.Token))
				{
					diskIo.TestDiskIO();

					var diskIoRequestAndResponse = new
					{
						Request = ioTestRequest,
						Result = diskIo.Result,
						DebugMsgs = debugInfo
					};

					Database.Documents.Put(AbstractDiskPerformanceTester.PerformanceResultDocumentKey, null, RavenJObject.FromObject(diskIoRequestAndResponse), new RavenJObject(), null);
				}
			});

			long id;
			Database.Tasks.AddTask(task, new TaskBasedOperationState(task), new TaskActions.PendingTaskDescription
			{
				StartTime = SystemTime.UtcNow,
				TaskType = TaskActions.PendingTaskType.IoTest,
				Payload = "Disk performance test"
			}, out id, killTaskCts);

			return GetMessageWithObject(new
			{
				OperationId = id
			});
		}

		[HttpPost]
		[RavenRoute("admin/low-memory-notification")]
		public HttpResponseMessage LowMemoryNotification()
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("Low memory simulation is only possible from the system database", HttpStatusCode.BadRequest);

			MemoryStatistics.SimulateLowMemoryNotification();

			return GetEmptyMessage();
		}
	}
}