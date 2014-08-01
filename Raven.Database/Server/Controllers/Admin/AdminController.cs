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
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util;
using Raven.Database.Actions;
using Raven.Database.Backup;
using Raven.Database.Config;
using System.Net.Http;

using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Plugins.Builtins;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.Security;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using Voron.Impl.Backup;

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
		[Route("admin/backup")]
		[Route("databases/{databaseName}/admin/backup")]
		public async Task<HttpResponseMessage> Backup()
		{
			var backupRequest = await ReadJsonObjectAsync<BackupRequest>();
			var incrementalString = InnerRequest.RequestUri.ParseQueryString()["incremental"];
			bool incrementalBackup;
			if (bool.TryParse(incrementalString, out incrementalBackup) == false)
				incrementalBackup = false;

            if (backupRequest.DatabaseDocument == null && Database.Name != null)
            {
                if (Database.Name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase))
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
		[Route("admin/restore")]
		[Route("databases/{databaseName}/admin/restore")]
		public async Task<HttpResponseMessage> Restore()
		{
			if (EnsureSystemDatabase() == false)
				return GetMessageWithString("Restore is only possible from the system database", HttpStatusCode.BadRequest);

            var restoreStatus = new RestoreStatus{Messages = new List<string>()};

			var restoreRequest = await ReadJsonObjectAsync<RestoreRequest>();

			DatabaseDocument databaseDocument = null;

			var databaseDocumentPath = Path.Combine(restoreRequest.BackupLocation, "Database.Document");
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

				return GetMessageWithString(errorMessage,HttpStatusCode.BadRequest);
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

			if (File.Exists(Path.Combine(restoreRequest.BackupLocation, BackupMethods.Filename)))
                ravenConfiguration.DefaultStorageTypeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
            else if (Directory.Exists(Path.Combine(restoreRequest.BackupLocation, "new")))
				ravenConfiguration.DefaultStorageTypeName = typeof (Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;

			ravenConfiguration.CustomizeValuesForTenant(databaseName);
			ravenConfiguration.Initialize();

			string documentDataDir;
			ravenConfiguration.DataDirectory = ResolveTenantDataDirectory(restoreRequest.DatabaseLocation, databaseName, out documentDataDir);
			restoreRequest.DatabaseLocation = ravenConfiguration.DataDirectory;
			DatabasesLandlord.SystemDatabase.Documents.Delete(RestoreStatus.RavenRestoreStatusDocumentKey, null, null);
		    
            bool defrag;
		    if (bool.TryParse(GetQueryStringValue("defrag"), out defrag))
		        restoreRequest.Defrag = defrag;

            await Task.Factory.StartNew(() =>
            {
                MaintenanceActions.Restore(ravenConfiguration,restoreRequest,
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
                databaseDocument.Id = databaseName;
                DatabasesLandlord.Protect(databaseDocument);
                DatabasesLandlord.SystemDatabase.Documents.Put("Raven/Databases/" + databaseName, null, RavenJObject.FromObject(databaseDocument),
                    new RavenJObject(), null);

                restoreStatus.Messages.Add("The new database was created");
                DatabasesLandlord.SystemDatabase.Documents.Put(RestoreStatus.RavenRestoreStatusDocumentKey, null,
                    RavenJObject.FromObject(restoreStatus), new RavenJObject(), null);
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
				documentDataDir = Path.Combine("~\\Databases", databaseName);
				return Path.Combine(baseDataPath, documentDataDir.Substring(2));
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

			DatabasesLandlord.Lock(db, () => DatabasesLandlord.SystemDatabase.TransactionalStorage.Compact(configuration));

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
		public HttpResponseMessage Stats()
		{
			if (Database != DatabasesLandlord.SystemDatabase)
				return GetMessageWithString("Admin stats can only be had from the root database", HttpStatusCode.NotFound);

		    var allDbs = new List<DocumentDatabase>();
            var allFs = new List<RavenFileSystem>();
            DatabasesLandlord.ForAllDatabases(allDbs.Add);
            FileSystemsLandlord.ForAllFileSystems(allFs.Add);
		    var currentConfiguration = DatabasesLandlord.SystemConfiguration;

            
            var stats =  new AdminStatistics
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
                LoadedDatabases =
                    from documentDatabase in allDbs
                    let indexStorageSize = documentDatabase.GetIndexStorageSizeOnDisk()
                    let transactionalStorageSize = documentDatabase.GetTransactionalStorageSizeOnDisk()
                    let totalDatabaseSize = indexStorageSize + transactionalStorageSize.AllocatedSizeInBytes
                    let lastUsed = DatabasesLandlord.LastRecentlyUsed.GetOrDefault(documentDatabase.Name ?? Constants.SystemDatabase)
                    select new LoadedDatabaseStatistics
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

                        DatabaseTransactionVersionSizeInMB = ConvertBytesToMBs(documentDatabase.TransactionalStorage.GetDatabaseTransactionVersionSizeInBytes()),
                        Metrics = documentDatabase.CreateMetrics()
                    },
                LoadedFileSystems = from fileSystem in allFs
                   select fileSystem.GetFileSystemStats()
            };

            return GetMessageWithObject(stats);
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
		[Route("admin/detailed-storage-breakdown")]
		[Route("databases/{databaseName}/admin/detailed-storage-breakdown")]
		public HttpResponseMessage DetailedStorageBreakdown()
		{
			var x = Database.TransactionalStorage.ComputeDetailedStorageInformation();
			return GetMessageWithObject(x);
		}

        [HttpPost]
        [HttpGet]
        [Route("admin/gc")]
        public HttpResponseMessage Gc()
        {
            if (EnsureSystemDatabase() == false)
                return GetMessageWithString("Garbage Collection is only possiable from the system database", HttpStatusCode.BadRequest);


            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
            DatabasesLandlord.ForAllDatabases(documentDatabase => documentDatabase.TransactionalStorage.ClearCaches());
            GC.WaitForPendingFinalizers();

            return GetMessageWithString("GC Done");
        }

        [HttpGet]
        [HttpPost]
		[Route("admin/loh-compaction")]
        public HttpResponseMessage LohCompaction()
		{
            if (EnsureSystemDatabase() == false)
                return GetMessageWithString("Large Object Heap Garbage Collection is only possiable from the system database", HttpStatusCode.BadRequest);


		    Action<DocumentDatabase> clearCaches = documentDatabase => documentDatabase.TransactionalStorage.ClearCaches();
            Action afterCollect = () => DatabasesLandlord.ForAllDatabases(clearCaches);
			
			RavenGC.CollectGarbage(true, afterCollect);

            return GetMessageWithString("LOH GC Done");
        }

		[HttpGet]
		[Route("admin/tasks")]
		[Route("databases/{databaseName}/admin/tasks")]
		public HttpResponseMessage Tasks()
		{
			return GetMessageWithObject(FilterOutTasks(Database.StartupTasks));
		}

		private static IEnumerable<string> FilterOutTasks(OrderedPartCollection<IStartupTask> tasks)
		{
			return tasks.Select(task => task.GetType().FullName).Where(t => !TasksToFilterOut.Contains(t)).ToList();
		}

        [HttpGet]
        [Route("admin/killQuery")]
        [Route("databases/{databaseName}/admin/killQuery")]
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
		[Route("admin/debug/info-package")]
		public HttpResponseMessage InfoPackage()
		{
			var tempFileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            try
            {
                using (var file = new FileStream(tempFileName, FileMode.Create))
                using (var package = new ZipArchive(file, ZipArchiveMode.Create))
                {
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
	}
}