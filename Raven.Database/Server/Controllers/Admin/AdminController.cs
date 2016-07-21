using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
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
using Raven.Database.Actions;
using Raven.Database.Backup;
using Raven.Database.Config;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;

using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Database.DiskIO;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server.Connections;
using Raven.Database.FileSystem;
using Raven.Database.Server.Security;
using Raven.Database.Server.WebApi.Attributes;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using Voron.Impl.Backup;

using Raven.Database.Bundles.Replication.Data;
using Raven.Database.Bundles.Replication.Impl;
using Raven.Database.Counters.Replication;
using Raven.Database.FileSystem.Synchronization;
using Raven.Database.Smuggler;
using MaintenanceActions = Raven.Database.Actions.MaintenanceActions;

namespace Raven.Database.Server.Controllers.Admin
{
    [RoutePrefix("")]
    public class AdminController : BaseAdminDatabaseApiController
    {
        private static readonly HashSet<string> TasksToFilterOut = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                                                                   {
                                                                      typeof(AuthenticationForCommercialUseOnly).FullName,
                                                                      typeof(RemoveBackupDocumentStartupTask).FullName
                                                                   };

        [HttpGet]
        [RavenRoute("admin/generate-oauth-certificate")]
        public HttpResponseMessage GenerateOAuthCertificate()
        {
            string certificate;
            using (var rsa = new RSACryptoServiceProvider())
                certificate = Convert.ToBase64String(rsa.ExportCspBlob(true));

            var message = GetEmptyMessage();
            message.Content = new StringContent(certificate, Encoding.UTF8);
            message.Content.Headers.ContentDisposition = new ContentDispositionHeaderValue("attachment")
            {
                FileName = "oauth-certificate.txt"
            };

            message.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            return message;
        }

        [HttpGet]
        [RavenRoute("admin/cluster-statistics")]
        public HttpResponseMessage GetClusterStatistics()
        {
            return GetMessageWithObject(ClusterManager.Engine.EngineStatistics);
        }

        [HttpPost]
        [RavenRoute("admin/serverSmuggling")]
        public async Task<HttpResponseMessage> ServerSmuggling()
        {
            var request = await ReadJsonObjectAsync<ServerSmugglerRequest>().ConfigureAwait(false);
            var targetStore = CreateStore(request.TargetServer);

            var status = new ServerSmugglingOperationState();
            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                try
                {
                    foreach (var serverSmugglingItem in request.Config)
                    {
                        status.Messages.Add("Smuggling database " + serverSmugglingItem.Name);
                        var documentKey = Constants.Database.Prefix + serverSmugglingItem.Name;
                        if (targetStore.DatabaseCommands.Head(documentKey) == null)
                        {
                            var databaseJson = Database.Documents.Get(documentKey, null);
                            var databaseDocument = databaseJson.ToJson().JsonDeserialization<DatabaseDocument>();
                            databaseDocument.Id = documentKey;
                            DatabasesLandlord.Unprotect(databaseDocument);
                            targetStore.DatabaseCommands.GlobalAdmin.CreateDatabase(databaseDocument);
                        }

                        var source = await DatabasesLandlord.GetResourceInternal(serverSmugglingItem.Name).ConfigureAwait(false);

                        var dataDumper = new DatabaseDataDumper(source, new SmugglerDatabaseOptions
                        {
                            Incremental = serverSmugglingItem.Incremental,
                            StripReplicationInformation = serverSmugglingItem.StripReplicationInformation,
                            ShouldDisableVersioningBundle = serverSmugglingItem.ShouldDisableVersioningBundle,
                        });

                        await dataDumper.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
                        {
                            To = new RavenConnectionStringOptions
                            {
                                Url = request.TargetServer.Url,
                                DefaultDatabase = serverSmugglingItem.Name
                            },
                            ReportProgress = message => status.Messages.Add(message)
                        }).ConfigureAwait(false);
                    }

                    status.Messages.Add("Server smuggling completed successfully. Selected databases have been smuggled.");
                    status.MarkCompleted();
                }
                catch (Exception e)
                {
                    status.Messages.Add("Error: " + e.Message);
                    status.MarkFaulted(e.Message);
                    throw;
                }
            }, cts.Token);

            long id;
            Database.Tasks.AddTask(task, status, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.ServerSmuggling,
                Description = "Server smuggling"

            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        private class ServerSmugglingOperationState : OperationStateBase
        {
            public ServerSmugglingOperationState()
            {
                Messages = new List<string>();
            }

            public List<string> Messages { get; private set; }
        }

        private static DocumentStore CreateStore(ServerConnectionInfo connection)
        {
            var store = new DocumentStore
            {
                Url = connection.Url,
                ApiKey = connection.ApiKey,
                Credentials = connection.Credentials,
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately,
                    ShouldCacheRequest = s => false,
                    ShouldAggressiveCacheTrackChanges = false,
                    ShouldSaveChangesForceAggressiveCacheCheck = false,
                }
            };
            store.Initialize(ensureDatabaseExists: false);
            store.JsonRequestFactory.DisableAllCaching();
            return store;
        }

        [HttpPost]
        [RavenRoute("admin/backup")]
        [RavenRoute("databases/{databaseName}/admin/backup")]
        public async Task<HttpResponseMessage> Backup()
        {
            var backupRequest = await ReadJsonObjectAsync<DatabaseBackupRequest>().ConfigureAwait(false);
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

            var cts = new CancellationTokenSource();
            var state = new ResourceBackupState();

            var task = Database.Maintenance.StartBackup(backupRequest.BackupLocation, incrementalBackup, backupRequest.DatabaseDocument, state, cts.Token);
            task.ContinueWith(_ => cts.Dispose());

            long id;
            DatabasesLandlord.SystemDatabase.Tasks.AddTask(task, state, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.BackupDatabase,
                Description = "Backup to: " + backupRequest.BackupLocation
            }, out id, cts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
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

            var restoreRequest = await ReadJsonObjectAsync<DatabaseRestoreRequest>().ConfigureAwait(false);

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
                                await Task.Delay(500, token).ConfigureAwait(false);
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
                    databaseDocument.Settings.Remove(Constants.RavenIndexPath);
                    databaseDocument.Settings.Remove(Constants.RavenEsentLogsPath);
                    databaseDocument.Settings.Remove(Constants.RavenTxJournalPath);

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
                    var aggregateException = e as AggregateException;
                    var exception = aggregateException != null ? aggregateException.ExtractSingleInnerException() : e;

                    restoreStatus.State = RestoreStatusState.Faulted;
                    restoreStatus.Messages.Add("Unable to restore database " + exception.Message);
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
                Description = "Restoring database " + databaseName + " from " + restoreRequest.BackupLocation
            }, out id);


            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
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

            var databaseDocumentAsJson = DatabasesLandlord.SystemDatabase.Documents.Get(Constants.Database.Prefix + databaseName, null);
            var databaseDocument = databaseDocumentAsJson.DataAsJson.JsonDeserialization<DatabaseDocument>();

            var bundles = databaseDocument.Settings[Constants.ActiveBundles].GetSemicolonSeparatedValues();
            bundles.Add("Replication");

            databaseDocument.Settings[Constants.ActiveBundles] = string.Join(";", bundles);

            DatabasesLandlord
                    .SystemDatabase
                    .Documents
                    .Put(
                        Constants.Database.Prefix + databaseName,
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

        [HttpGet]
        [RavenRoute("admin/license/connectivity")]
        public HttpResponseMessage CheckConnectivityToLicenseServer()
        {
            var request = (HttpWebRequest)WebRequest.Create("https://licensing.ravendb.net/Subscriptions.svc");
            try
            {
                request.Timeout = 5000;
                using (var response = (HttpWebResponse)request.GetResponse())
                {
                    return GetMessageWithObject(new { Success = response.StatusCode == HttpStatusCode.OK });
                }
            }
            catch (Exception e)
            {
                return GetMessageWithObject(new { Success = false, Exception = e.Message });
            }
        }

        [HttpGet]
        [RavenRoute("admin/license/forceUpdate")]
        public HttpResponseMessage ForceLicenseUpdate()
        {
            Database.ForceLicenseUpdate();
            DatabasesLandlord.ForAllDatabases(database =>
            {
                database.WorkContext.ShouldNotifyAboutWork(() => "License update");
                database.WorkContext.NotifyAboutWork();
            });
            return GetEmptyMessage();
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

                    var targetDb = AsyncHelpers.RunSync(() => DatabasesLandlord.GetResourceInternal(db));

                    DatabasesLandlord.Lock(db, () => targetDb.TransactionalStorage.Compact(configuration, msg =>
                    {
                        bool skipProgressReport = false;
                        bool isProgressReport = false;
                        if (IsUpdateMessage(msg))
                        {
                            isProgressReport = true;
                            var now = SystemTime.UtcNow;
                            compactStatus.LastProgressMessageTime = compactStatus.LastProgressMessageTime ?? DateTime.MinValue;
                            var timeFromLastUpdate = (now - compactStatus.LastProgressMessageTime.Value);
                            if (timeFromLastUpdate >= ReportProgressInterval)
                            {
                                compactStatus.LastProgressMessageTime = now;
                                compactStatus.LastProgressMessage = msg;
                            }
                            else skipProgressReport = true;

                        }
                        if (!skipProgressReport)
                        {
                            if (!isProgressReport) compactStatus.Messages.Add(msg);
                            DatabasesLandlord.SystemDatabase.Documents.Put(CompactStatus.RavenDatabaseCompactStatusDocumentKey(db), null,
                                RavenJObject.FromObject(compactStatus), new RavenJObject(), null);
                        }

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
                Description = "Compact database " + db,
            }, out id);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        private static bool IsUpdateMessage(string msg)
        {
            if (String.IsNullOrEmpty(msg)) return false;
            //Here we check if we the message is in voron update format
            if (msg.StartsWith(VoronProgressString)) return true;
            //Here we check if we the messafe is in esent update format
            if (msg.Length > 42 && String.Compare(msg, 32, EsentProgressString, 0, 10) == 0) return true;
            return false;
        }
        private static TimeSpan ReportProgressInterval = TimeSpan.FromSeconds(1);
        private static string EsentProgressString = "JET_SNPROG";
        private static string VoronProgressString = "Copied";

        [HttpGet]
        [RavenRoute("admin/indexingStatus")]
        [RavenRoute("databases/{databaseName}/admin/indexingStatus")]
        public HttpResponseMessage IndexingStatus()
        {
            string mappingDisableStatus;
            string reducingDisableStatus;
            if (Database.IsIndexingDisabled())
            {
                mappingDisableStatus = reducingDisableStatus = "Disabled";
            }
            else
            {
                mappingDisableStatus = Database.WorkContext.RunIndexing ? "Mapping" : "Paused";
                reducingDisableStatus = Database.WorkContext.RunReducing ? "Reducing" : "Paused";
            }

            return GetMessageWithObject(new IndexingStatus
            {
                MappingStatus = mappingDisableStatus,
                ReducingStatus = reducingDisableStatus
            });
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

            Database.SpinBackgroundWorkers(true);
        }

        [HttpPost]
        [RavenRoute("admin/stopIndexing")]
        [RavenRoute("databases/{databaseName}/admin/stopIndexing")]
        public void StopIndexing()
        {
            Database.StopIndexingWorkers(true);
        }


        [HttpPost]
        [RavenRoute("admin/startReducing")]
        [RavenRoute("databases/{databaseName}/admin/startReducing")]
        public void StartReducing()
        {
            Database.SpinReduceWorker();
        }

        [HttpPost]
        [RavenRoute("admin/stopReducing")]
        [RavenRoute("databases/{databaseName}/admin/stopReducing")]
        public void StopReducing()
        {
            Database.StopReduceWorkers();
        }

        [HttpGet]
        [RavenRoute("admin/debug/auto-tuning-info")]
        [RavenRoute("databases/{databaseName}/admin/debug/auto-tuning-info")]
        public HttpResponseMessage DebugAutoTuningInfo()
        {
            return GetMessageWithObject(new AutoTunerInfo()
            {
                Reason = Database.AutoTuningTrace.ToList(),
                LowMemoryCallsRecords = MemoryStatistics.LowMemoryCallRecords.ToList(),
                CpuUsageCallsRecords = CpuStatistics.CpuUsageCallsRecordsQueue.ToList()
            });
        }

        [HttpGet]
        [RavenRoute("admin/stats")]
        public HttpResponseMessage Stats()
        {
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
            var detailedReport = GetQueryStringValue("DetailedReport");
            bool isDetailedReport;

            var cts = new CancellationTokenSource();
            var state = new InternalStorageBreakdownState();

            var computeExactSizes = detailedReport != null && bool.TryParse(detailedReport, out isDetailedReport) && isDetailedReport;

            var computeTask = Task.Factory.StartNew(() =>
            {
                try
                {
                    var reportResults = Database.TransactionalStorage.ComputeDetailedStorageInformation(computeExactSizes, msg => state.MarkProgress(msg), cts.Token);
                    state.ReportResults = reportResults;
                    state.MarkCompleted();
                }
                catch (Exception e)
                {
                    state.MarkFaulted(e.Message, e);
                }
            });
            long taskId;
            Database.Tasks.AddTask(computeTask, state, new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.StorageBreakdown,
                Description = "Detailed Storage Breakdown"
            }, out taskId, cts);

            return GetMessageWithObject(new
            {
                OperationId = taskId
            }, HttpStatusCode.Accepted);
        }

        [HttpPost]
        [HttpGet]
        [RavenRoute("admin/gc")]
        public HttpResponseMessage Gc()
        {
            Action<DocumentDatabase> clearCaches = documentDatabase => documentDatabase.TransactionalStorage.ClearCaches();
            Action afterCollect = () => DatabasesLandlord.ForAllDatabases(clearCaches);

            RavenGC.CollectGarbage(false, afterCollect, true);

            return GetMessageWithString("GC Done");
        }

        [HttpGet]
        [HttpPost]
        [RavenRoute("admin/loh-compaction")]
        public HttpResponseMessage LohCompaction()
        {
            Action<DocumentDatabase> clearCaches = documentDatabase => documentDatabase.TransactionalStorage.ClearCaches();
            Action afterCollect = () => DatabasesLandlord.ForAllDatabases(clearCaches);

            RavenGC.CollectGarbage(true, afterCollect, true);

            return GetMessageWithString("LOH GC Done");
        }

        [HttpGet]
        [RavenRoute("admin/verify-principal")]
        public HttpResponseMessage VerifyPrincipal(string mode, string principal)
        {
            switch (mode)
            {
                case "user":
                    return GetMessageWithObject(new { Valid = AccountVerifier.UserExists(principal) });
                case "group":
                    return GetMessageWithObject(new { Valid = AccountVerifier.GroupExists(principal) });
                default:
                    return GetMessageWithString("Unhandled mode: " + mode, HttpStatusCode.BadRequest);
            }
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
            var tempFileName = Path.Combine(Database.Configuration.TempPath, Path.GetRandomFileName());
            try
            {
                var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
                jsonSerializer.Formatting = Formatting.Indented;

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
                        DebugInfoProvider.CreateInfoPackageForDatabase(package, database, RequestManager, ClusterManager, prefix + "/");
                    });

                    bool stacktrace;
                    if (bool.TryParse(GetQueryStringValue("stacktrace"), out stacktrace) && stacktrace)
                        DumpStacktrace(package);
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
            var stacktrace = package.CreateEntry("stacktraces.txt", CompressionLevel.Optimal);

            var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
            jsonSerializer.Formatting = Formatting.Indented;

            using (var stacktraceStream = stacktrace.Open())
            {
                string ravenDebugDir = null;

                var output = string.Empty;
                try
                {
                    if (Debugger.IsAttached) throw new InvalidOperationException("Cannot get stacktraces when debugger is attached");

                    ravenDebugDir = Path.Combine(Database.Configuration.TempPath, Path.GetRandomFileName());
                    var ravenDebugExe = Path.Combine(ravenDebugDir, "Raven.Debug.exe");
                    var ravenDbgHelp = Path.Combine(ravenDebugDir, "dbghelp.dll");
                    var ravenDebugOutput = Path.Combine(ravenDebugDir, "stacktraces.txt");

                    Directory.CreateDirectory(ravenDebugDir);

                    if (Environment.Is64BitProcess)
                    {
                        ExtractResource("Raven.Database.Util.Raven.Debug.x64.dbghelp.dll", ravenDbgHelp);
                        ExtractResource("Raven.Database.Util.Raven.Debug.x64.Raven.Debug.exe", ravenDebugExe);
                    }
                    else
                    {
                        ExtractResource("Raven.Database.Util.Raven.Debug.x86.dbghelp.dll", ravenDbgHelp);
                        ExtractResource("Raven.Database.Util.Raven.Debug.x86.Raven.Debug.exe", ravenDebugExe);
                    }

                    var process = new Process
                    {
                        StartInfo = new ProcessStartInfo
                        {
                            Arguments = string.Format("--pid={0} --stacktrace --output=\"{1}\"", Process.GetCurrentProcess().Id, ravenDebugOutput),
                            FileName = ravenDebugExe,
                            WindowStyle = ProcessWindowStyle.Normal,
                            LoadUserProfile = false,
                            RedirectStandardError = true,
                            RedirectStandardOutput = true,
                            UseShellExecute = false
                        },
                        EnableRaisingEvents = true
                    };



                    process.OutputDataReceived += (sender, args) => output += args.Data;
                    process.ErrorDataReceived += (sender, args) => output += args.Data;

                    process.Start();

                    process.BeginErrorReadLine();
                    process.BeginOutputReadLine();

                    process.WaitForExit();

                    if (process.ExitCode != 0)
                    {
                        Log.Error("Could not read stacktraces. Message: " + output);
                        throw new InvalidOperationException("Could not read stacktraces.");
                    }

                    using (var stackDumpOutputStream = File.Open(ravenDebugOutput, FileMode.Open))
                    {
                        stackDumpOutputStream.CopyTo(stacktraceStream);
                    }
                }
                catch (Exception ex)
                {
                    var streamWriter = new StreamWriter(stacktraceStream);
                    jsonSerializer.Serialize(streamWriter, new { Error = ex.Message, Details = output });
                    streamWriter.Flush();
                }
                finally
                {
                    if (ravenDebugDir != null && Directory.Exists(ravenDebugDir)) IOExtensions.DeleteDirectory(ravenDebugDir);
                }

                stacktraceStream.Flush();
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
                    bool watchStack = tokens.Length == 3 && tokens[2] == "watch-stack";
                    LogLevel level;
                    if (Enum.TryParse(tokens[1], out level))
                    {
                        return Tuple.Create(tokens[0], level, watchStack);
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
                connectionState.EnableLogging(categoryAndLevel.Item1, categoryAndLevel.Item2, categoryAndLevel.Item3);
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
            var json = await ReadJsonAsync().ConfigureAwait(false);
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

            Database.Documents.Delete(AbstractDiskPerformanceTester.PerformanceResultDocumentKey, null, null);

            var killTaskCts = new CancellationTokenSource();

            var operationStatus = new RavenJObject();

            var task = Task.Factory.StartNew(() =>
            {
                var debugInfo = new List<string>();
                var hasFaulted = false;
                var errors = new Exception[0];
                using (var diskIo = AbstractDiskPerformanceTester.ForRequest(ioTestRequest, msg =>
                {
                    debugInfo.Add(msg);
                    operationStatus["Progress"] = msg;
                }, killTaskCts.Token))
                {
                    diskIo.TestDiskIO();

                    // reset operation status after test
                    operationStatus.Remove("Progress");

                    RavenJObject diskPerformanceRequestResponseDoc;

                    if (diskIo.HasFailed == false)
                    {
                        diskPerformanceRequestResponseDoc = RavenJObject.FromObject(new
                        {
                            Request = ioTestRequest,
                            // ReSharper disable once RedundantAnonymousTypePropertyName
                            Result = diskIo.Result,
                            DebugMsgs = debugInfo
                        });
                    }
                    else
                    {
                        diskPerformanceRequestResponseDoc = RavenJObject.FromObject(new
                        {
                            Request = ioTestRequest,
                            // ReSharper disable once RedundantAnonymousTypePropertyName
                            Result = diskIo.Result,
                            DebugMsgs = debugInfo,
                            diskIo.HasFailed,
                            diskIo.Errors
                        });

                        hasFaulted = true;
                        errors = diskIo.Errors.ToArray();
                    }

                    Database.Documents.Put(AbstractDiskPerformanceTester.PerformanceResultDocumentKey, null, diskPerformanceRequestResponseDoc, new RavenJObject(), null);

                    if (hasFaulted && errors.Length > 0)
                        throw errors.First();

                    if (hasFaulted)
                        throw new Exception("Disk I/O test has failed. See log for more details.");
                }
            }, killTaskCts.Token);

            long id;
            Database.Tasks.AddTask(task, new TaskBasedOperationState(task, () => operationStatus), new TaskActions.PendingTaskDescription
            {
                StartTime = SystemTime.UtcNow,
                TaskType = TaskActions.PendingTaskType.IoTest,
                Description = "Disk performance test"
            }, out id, killTaskCts);

            return GetMessageWithObject(new
            {
                OperationId = id
            }, HttpStatusCode.Accepted);
        }

        [HttpPost]
        [RavenRoute("admin/low-memory-notification")]
        public HttpResponseMessage LowMemoryNotification()
        {
            MemoryStatistics.SimulateLowMemoryNotification();

            return GetEmptyMessage();
        }

        [HttpGet]
        [RavenRoute("admin/low-memory-handlers-statistics")]
        public HttpResponseMessage GetLowMemoryStatistics()
        {
            return GetMessageWithObject(MemoryStatistics.GetLowMemoryHandlersStatistics().GroupBy(x => x.DatabaseName).Select(x => new
            {
                DatabaseName = x.Key,
                Types = x.GroupBy(y => y.Name).Select(y => new
                {
                    MemoryHandlerName = y.Key,
                    MemoryHandlers = y.Select(z => new
                    {
                        z.EstimatedUsedMemory,
                        z.Metadata
                    })
                })
            }));
        }

        [HttpPost]
        [RavenRoute("admin/replication/topology/global")]
        public async Task<HttpResponseMessage> GlobalReplicationTopology()
        {
            var request = await ReadJsonObjectAsync<GlobalReplicationTopologyRequest>().ConfigureAwait(false);

            ReplicationTopology databasesTopology = null;
            SynchronizationTopology filesystemsTopology = null;
            CountersReplicationTopology counterStoragesTopology = null;

            if (request.Databases)
                databasesTopology = CollectReplicationTopology();

            if (request.Filesystems)
                filesystemsTopology = CollectFilesystemSynchronizationTopology();

            if (request.Counters)
                counterStoragesTopology = CollectionCountersReplicationTopology();

            return GetMessageWithObject(new
            {
                Databases = databasesTopology,
                FileSystems = filesystemsTopology,
                Counters = counterStoragesTopology
            });
        }

        private ReplicationTopology CollectReplicationTopology()
        {
            var mergedTopology = new ReplicationTopology();

            int nextPageStart = 0;
            var databases = DatabasesLandlord.SystemDatabase.Documents
                .GetDocumentsWithIdStartingWith(DatabasesLandlord.ResourcePrefix, null, null, 0,
                    int.MaxValue, CancellationToken.None, ref nextPageStart);

            var databaseNames = databases
                .Select(database =>
                    database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(DatabasesLandlord.ResourcePrefix, string.Empty)).ToHashSet();

            DatabasesLandlord.ForAllDatabases(db =>
            {
                if (db.IsSystemDatabase())
                    return;

                var databaseId = DatabasesLandlord.GetDatabaseId(db.Name);
                if (databaseId != null)
                    mergedTopology.LocalDatabaseIds.Add(databaseId.Value);

                databaseNames.Remove(db.Name);
                var replicationSchemaDiscoverer = new ReplicationTopologyDiscoverer(db, new RavenJArray(), 10, Log);
                var node = replicationSchemaDiscoverer.Discover();
                var topology = node.Flatten();
                topology.Servers.ForEach(s => mergedTopology.Servers.Add(s));
                topology.Connections.ForEach(connection =>
                {
                    if (mergedTopology.Connections.Any(c => c.Source == connection.Source && c.Destination == connection.Destination) == false)
                    {
                        mergedTopology.Connections.Add(connection);
                    }
                });
            });

            mergedTopology.SkippedResources = databaseNames;
            return mergedTopology;
        }

        private SynchronizationTopology CollectFilesystemSynchronizationTopology()
        {
            var mergedTopology = new SynchronizationTopology();

            int nextPageStart = 0;
            var filesystems = DatabasesLandlord.SystemDatabase.Documents
                .GetDocumentsWithIdStartingWith(FileSystemsLandlord.ResourcePrefix, null, null, 0,
                    int.MaxValue, CancellationToken.None, ref nextPageStart);

            var filesystemsNames = filesystems
                .Select(database =>
                    database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(FileSystemsLandlord.ResourcePrefix, string.Empty)).ToHashSet();

            FileSystemsLandlord.ForAllFileSystems(fs =>
            {
                filesystemsNames.Remove(fs.Name);

                var synchronizationSchemaDiscoverer = new SynchronizationTopologyDiscoverer(fs, new RavenJArray(), 10, Log);
                var node = synchronizationSchemaDiscoverer.Discover();
                var topology = node.Flatten();
                topology.Servers.ForEach(s => mergedTopology.Servers.Add(s));
                topology.Connections.ForEach(connection =>
                {
                    if (mergedTopology.Connections.Any(c => c.Source == connection.Source && c.Destination == connection.Destination) == false)
                    {
                        mergedTopology.Connections.Add(connection);
                    }
                });
            });

            mergedTopology.SkippedResources = filesystemsNames;

            return mergedTopology;
        }

        private CountersReplicationTopology CollectionCountersReplicationTopology()
        {
            var mergedTopology = new CountersReplicationTopology();

            int nextPageStart = 0;
            var counters = DatabasesLandlord.SystemDatabase.Documents
                .GetDocumentsWithIdStartingWith(CountersLandlord.ResourcePrefix, null, null, 0,
                    int.MaxValue, CancellationToken.None, ref nextPageStart);

            var countersNames = counters
                .Select(database =>
                    database.Value<RavenJObject>("@metadata").Value<string>("@id").Replace(CountersLandlord.ResourcePrefix, string.Empty)).ToHashSet();

            CountersLandlord.ForAllCountersInCacheOnly(cs =>
            {
                countersNames.Remove(cs.Name);

                var schemaDiscoverer = new CountersReplicationTopologyDiscoverer(cs, new RavenJArray(), 10, Log);
                var node = schemaDiscoverer.Discover();
                var topology = node.Flatten();
                topology.Servers.ForEach(s => mergedTopology.Servers.Add(s));
                topology.Connections.ForEach(connection =>
                {
                    if (mergedTopology.Connections.Any(c => c.Source == connection.Source && c.Destination == connection.Destination) == false)
                    {
                        mergedTopology.Connections.Add(connection);
                    }
                });
            });

            mergedTopology.SkippedResources = countersNames;

            return mergedTopology;
        }

        [HttpGet]
        [RavenRoute("admin/dump")]
        public HttpResponseMessage Dump()
        {
            var stop = GetQueryStringValue("stop");
            if (!string.IsNullOrEmpty(stop))
            {
                MiniDumper.Instance.StopTimer();
                return GetMessageWithString("Dump Timer Canceled", HttpStatusCode.Accepted);
            }

            var usage = GetQueryStringValue("usage");
            if (!string.IsNullOrEmpty(usage))
                return GetMessageWithString(MiniDumper.PrintUsage(), HttpStatusCode.Accepted);

            MiniDumper.Instance.StopTimer();

            int timerCount;
            int period = 0;
            bool useTimer =
                int.TryParse(GetQueryStringValue("timer"), out timerCount) &&
                int.TryParse(GetQueryStringValue("period"), out period);

            var options =
                MiniDumper.Option.WithThreadInfo |
                MiniDumper.Option.WithProcessThreadData;
            var ids = GetQueryStringValues("option");
            foreach (var id in ids)
            {
                if (!string.IsNullOrEmpty(id))
                {
                    options |= MiniDumper.StringToOption(id);
                }
            }

            bool useStats = !string.IsNullOrEmpty(GetQueryStringValue("stats"));

            try
            {
                if (useTimer == false)
                    return GetMessageWithString(MiniDumper.Instance.Write(options), HttpStatusCode.Accepted);
            }
            catch (Exception ex)
            {
                return GetMessageWithString(ex.Message, HttpStatusCode.ExpectationFailed);
            }

            var url = $"http://{Request.RequestUri.Host}:{Request.RequestUri.Port}";
            return GetMessageWithString(MiniDumper.Instance.StartTimer(timerCount, period, options, useStats, url), HttpStatusCode.Accepted);
        }
    }
}
