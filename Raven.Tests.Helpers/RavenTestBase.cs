//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.Composition.Primitives;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.FileSystem.Util;
using Raven.Database.Raft;
using Raven.Database.Server.Security;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers.Util;


namespace Raven.Tests.Helpers
{
    public abstract class RavenTestBase : IDisposable
    {
        protected readonly List<RavenDbServer> servers = new List<RavenDbServer>();
        protected readonly List<IDocumentStore> stores = new List<IDocumentStore>();
        protected readonly HashSet<string> pathsToDelete = new HashSet<string>();

        protected readonly HashSet<string> DatabaseNames = new HashSet<string> { Constants.SystemDatabase };

        private static int pathCount;

        private static bool checkedAsyncVoid;

        protected RavenTestBase()
        {
            if (checkedAsyncVoid == false)
            {
                checkedAsyncVoid = true;
                AssertNoAsyncVoidMethods(GetType().Assembly);
            }

            Environment.SetEnvironmentVariable(Constants.RavenDefaultQueryTimeout, "30");
            CommonInitializationUtil.Initialize();

            // Make sure to delete the Data folder which we be used by tests that do not call the NewDataPath from whatever reason.
            var dataFolder = FilePathTools.MakeSureEndsWithSlash(@"~\Databases".ToFullPath());
            ClearDatabaseDirectory(dataFolder);
            pathsToDelete.Add(dataFolder);
        }

        private static IEnumerable<Type> GetLoadableTypes(Assembly assembly)
        {
            if (assembly == null) throw new ArgumentNullException("assembly");
            try
            {
                return assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException e)
            {
                return e.Types.Where(t => t != null);
            }
        }

        private static IEnumerable<MethodInfo> GetAsyncVoidMethods(Assembly assembly)
        {
            return GetLoadableTypes(assembly)
              .SelectMany(type => type.GetMethods(
                BindingFlags.NonPublic
                | BindingFlags.Public
                | BindingFlags.Instance
                | BindingFlags.Static
                | BindingFlags.DeclaredOnly))
              .Where(method => method.GetCustomAttribute<AsyncStateMachineAttribute>() != null)
              .Where(method => method.ReturnType == typeof(void));
        }

        public static void AssertNoAsyncVoidMethods(Assembly assembly)
        {
            var messages = GetAsyncVoidMethods(assembly)
                .Select(method =>
                    String.Format("'{0}.{1}' is an async void method.",
                        method.DeclaringType.FullName,
                        method.Name))
                .ToList();
            if (messages.Any())
                throw new InvalidConstraintException("async void methods found!" + Environment.NewLine + String.Join(Environment.NewLine, messages));
        }

        ~RavenTestBase()
        {
            try
            {
                Dispose();
            }
            catch (Exception)
            {
                // nothing that we can do here
            }
        }

        protected string NewDataPath(string prefix = null, bool forceCreateDir = false)
        {
            if (prefix != null)
                prefix = prefix.Replace("<", "").Replace(">", "");

            var newDataDir = Path.GetFullPath(string.Format(@".\{1}-{0}-{2}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "TestDatabase", Interlocked.Increment(ref pathCount)));
            if (forceCreateDir && Directory.Exists(newDataDir) == false)
                Directory.CreateDirectory(newDataDir);
            pathsToDelete.Add(newDataDir);
            return newDataDir;
        }

        /// <summary>
        /// Creates a new Embeddable document store.
        /// </summary>
        /// <param name="runInMemory">Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost.<br/>Default: <b>true</b></param>
        /// <param name="requestedStorage">What storage type to use (see: RavenDB Storage engines).<br/>Allowed values: <b>vornon</b>, <b>esent</b>.<br/>Default: <b>voron</b></param>
        /// <param name="catalog">Custom bundles that are not provided by RavenDb.</param>
        /// <param name="dataDir">The path for the database directory. Can use ~\ as the root, in which case the path will start from the server base directory. <br/>Default: <b>~\Databases\System</b></param>
        /// <param name="enableAuthentication"></param>
        /// <param name="activeBundles">Semicolon separated list of bundles names, such as: 'Replication;Versioning'.<br/>Default: no bundles turned on.</param>
        /// <param name="port">The port to use when creating the http listener. Allowed: 1 - 65,536 or * (find first available port from 8079 and upward).<br/>Default: <b>8079</b></param>
        /// <param name="anonymousUserAccessMode">Determines what actions an anonymous user can do. Get - read only, All - read & write, None - allows access to only authenticated users, Admin - all (including administrative actions).<br/>Default: <b>Get</b></param>
        /// <param name="configureStore">An action delegate which allows you to configure the document store instance that is returned. eg. <code>configureStore: store => store.DefaultDatabase = "MasterDb"</code></param>
        /// <param name="databaseName">Name of the server that will show up on /admin/stats endpoint.</param>
        /// <param name="indexes">A collection of indexes to execute.</param>
        /// <param name="transformers">A collection of transformers to execute.</param>
        /// <param name="seedData">A collection of some fake data that will be automatically stored into the document store.</param>
        /// <param name="noStaleQueries">When you query an index, the query will wait for the index to complete it's indexing and not be stale -before- the query is executed.</param>
        /// <param name="conventions">The conventions to be used when creating a new embeddable document store</param>
        /// <remarks>Besides the document store being instantiated, it is also Initialized.<br/>Also, if you provide some indexes to be used, make sure you understand that they might be stale when you query them. To make sure you're querying against indexes that have completed their indexing (ie. index is not stale), use the <code>noStaleQueries</code> parameter to determine if you wish to query against a stale or not-stale query.</remarks>
        /// <returns>A new instance of an EmbeddableDocumentStore.</returns>
        public EmbeddableDocumentStore NewDocumentStore(
            bool runInMemory = true,
            string requestedStorage = null,
            ComposablePartCatalog catalog = null,
            string dataDir = null,
            bool enableAuthentication = false,
            string activeBundles = null,
            int? port = null,
            AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin,
            Action<EmbeddableDocumentStore> configureStore = null,
            [CallerMemberName] string databaseName = null,
            IEnumerable<AbstractIndexCreationTask> indexes = null,
            IEnumerable<AbstractTransformerCreationTask> transformers = null,
            IEnumerable<IEnumerable> seedData = null,
            bool noStaleQueries = false,
            DocumentConvention conventions = null)
        {
            databaseName = NormalizeDatabaseName(databaseName);

            var storageType = GetDefaultStorageType(requestedStorage);
            var dataDirectory = dataDir ?? NewDataPath(databaseName);
            var documentStore = new EmbeddableDocumentStore
            {
                UseEmbeddedHttpServer = port.HasValue,
                Conventions = conventions ?? new DocumentConvention()
            };

            ConfigurationHelper.ApplySettingsToConfiguration(documentStore.Configuration);

            documentStore.Configuration.DefaultStorageTypeName = storageType;
            documentStore.Configuration.DataDirectory = Path.Combine(dataDirectory, "System");
            documentStore.Configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true;
            documentStore.Configuration.RunInMemory = storageType.Equals("esent", StringComparison.OrdinalIgnoreCase) == false && runInMemory;
            documentStore.Configuration.Port = port ?? 8079;
            documentStore.Configuration.AnonymousUserAccessMode = anonymousUserAccessMode;

            documentStore.Configuration.FileSystem.DataDirectory = Path.Combine(dataDirectory, "FileSystem");
            documentStore.Configuration.Encryption.UseFips = ConfigurationHelper.UseFipsEncryptionAlgorithms;

            if (activeBundles != null)
            {
                documentStore.Configuration.Settings["Raven/ActiveBundles"] = activeBundles;
            }

            if (catalog != null)
            {
                documentStore.Configuration.Catalog.Catalogs.Add(catalog);
            }

            try
            {
                if (configureStore != null)
                {
                    configureStore(documentStore);
                }

                ModifyStore(documentStore);
                ModifyConfiguration(documentStore.Configuration);
                documentStore.Configuration.PostInit();
                documentStore.Initialize();

                if (enableAuthentication)
                {
                    EnableAuthentication(documentStore.SystemDatabase);
                }

                CreateDefaultIndexes(documentStore);

                if (indexes != null)
                {
                    ExecuteIndexes(indexes, documentStore);
                }

                if (noStaleQueries)
                {
                    documentStore.Listeners.RegisterListener(new NoStaleQueriesListener());
                }

                if (transformers != null)
                {
                    ExecuteTransformers(transformers, documentStore);
                }

                if (seedData != null)
                {
                    StoreSeedData(seedData, documentStore);
                }

                return documentStore;
            }
            catch (Exception)
            {
                // We must dispose of this object in exceptional cases, otherwise this test will break all the following tests.
                try
                {
                documentStore.Dispose();
                }
                catch (Exception exception)
                {
                    // we don't want to hide the original error
                    Console.WriteLine("Error when disposing of document store during error in initialization: " + exception);
                }
                throw;
            }
            finally
            {
                stores.Add(documentStore);
            }
        }

        public static void EnableAuthentication(DocumentDatabase database)
        {
            var license = GetLicenseByReflection(database);
            license.Error = false;
            license.Status = "Commercial";
            license.Attributes["ravenfs"] = "true";
            license.Attributes["counters"] = "true";
            // rerun this startup task
            database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First().Execute(database);
        }

        /// <summary>
        /// Creates a new document store connecting to a remote RavenDb server.
        /// </summary>
        /// <param name="fiddler">Are all requests to the remote RavenDb server passed through Fiddler? (NOTE: This is only* for a localhost RavenDb server).</param>
        /// <param name="ravenDbServer">A RavenDb server.</param>
        /// <param name="databaseName">Name of the server that will show up on /admin/stats endpoint.</param>
        /// <param name="runInMemory">Whatever the database should run purely in memory. When running in memory, nothing is written to disk and if the server is restarted all data will be lost.<br/>Default: <b>true</b></param>
        /// <param name="dataDirectory">The path for the database directory. Can use ~\ as the root, in which case the path will start from the server base directory. <br/>Default: <b>~\Databases\System</b></param>
        /// <param name="requestedStorage">What storage type to use (see: RavenDB Storage engines).<br/>Allowed values: <b>vornon</b>, <b>esent</b>.<br/>Default: <b>voron</b></param>
        /// <param name="enableAuthentication"></param>
        /// <param name="ensureDatabaseExists">For a multi-tenant RavenDb server, creates the database if it doesn't already exist.</param>
        /// <param name="configureStore">An action delegate which allows you to configure the document store instance that is returned. eg. <code>configureStore: store => store.DefaultDatabase = "MasterDb"</code></param>
        /// <param name="activeBundles">Semicolon separated list of bundles names, such as: 'Replication;Versioning'.<br/>Default: no bundles turned on.</param>
        /// <param name="indexes">A collection of indexes to execute.</param>
        /// <param name="transformers">A collection of transformers to execute.</param>
        /// <param name="seedData">A collection of some fake data that will be automatically stored into the document store.</param>
        /// <param name="noStaleQueries">When you query an index, the query will wait for the index to complete it's indexing and not be stale -before- the query is executred.</param>
        /// <returns></returns>
        public DocumentStore NewRemoteDocumentStore(bool fiddler = false,
            RavenDbServer ravenDbServer = null,
            [CallerMemberName] string databaseName = null,
            bool runInMemory = true,
            string dataDirectory = null,
            string requestedStorage = null,
            bool enableAuthentication = false,
            bool ensureDatabaseExists = true,
            Action<DocumentStore> configureStore = null,
            string activeBundles = null,
            IEnumerable<AbstractIndexCreationTask> indexes = null,
            IEnumerable<AbstractTransformerCreationTask> transformers = null,
            IEnumerable<IEnumerable> seedData = null,
            bool noStaleQueries = false)
        {
            databaseName = NormalizeDatabaseName(databaseName);

            checkPorts = true;
            ravenDbServer = ravenDbServer ?? GetNewServer(runInMemory: runInMemory,
                dataDirectory: dataDirectory,
                requestedStorage: requestedStorage,
                enableAuthentication: enableAuthentication,
                databaseName: databaseName,
                activeBundles: activeBundles);
            ModifyServer(ravenDbServer);
            var documentStore = new DocumentStore
            {
                Url = GetServerUrl(fiddler, ravenDbServer.SystemDatabase.ServerUrl),
                DefaultDatabase = databaseName
            };
            pathsToDelete.Add(Path.Combine(ravenDbServer.SystemDatabase.Configuration.DataDirectory, @"..\Databases"));
            stores.Add(documentStore);
            documentStore.AfterDispose += (sender, args) => ravenDbServer.Dispose();

            if (configureStore != null)
            {
                configureStore(documentStore);
            }

            ModifyStore(documentStore);

            documentStore.Initialize(ensureDatabaseExists);

            if (indexes != null)
            {
                ExecuteIndexes(indexes, documentStore);
            }

            if (noStaleQueries)
            {
                // When querying any map/reduce indexes, we'll wait until
                // the index has stopped being stale.
                documentStore.Listeners.RegisterListener(new NoStaleQueriesListener());
            }

            if (transformers != null)
            {
                ExecuteTransformers(transformers, documentStore);
            }

            if (seedData != null)
            {
                StoreSeedData(seedData, documentStore);
            }

            return documentStore;
        }

        protected RavenDbServer GetServer(int port = 8079)
        {
            return servers.First(x => x.SystemDatabase.Configuration.Port == port);
        }

        protected static string GetServerUrl(bool fiddler, string serverUrl)
        {
            if (fiddler)
            {
                if (Process.GetProcessesByName("fiddler").Any())
                    return serverUrl.Replace("localhost", "localhost.fiddler");
            }
            return serverUrl;
        }

        public static string GetDefaultStorageType(string requestedStorage = null)
        {
            string defaultStorageType;
            var envVar = Environment.GetEnvironmentVariable("raventest_storage_engine");
            if (string.IsNullOrEmpty(envVar) == false)
                defaultStorageType = envVar;
            else if (requestedStorage != null)
                defaultStorageType = requestedStorage;
            else
                defaultStorageType = "voron";
            return defaultStorageType;
        }

        protected bool checkPorts = false;

        protected RavenDbServer GetNewServer(int port = 8079,
            string dataDirectory = null,
            bool runInMemory = true,
            string requestedStorage = null,
            bool enableAuthentication = false,
            string activeBundles = null,
            Action<RavenDBOptions> configureServer = null,
            Action<InMemoryRavenConfiguration> configureConfig = null,
            [CallerMemberName] string databaseName = null)
        {
            if (databaseName == ".ctor")
                databaseName = "ctor";
            databaseName = NormalizeDatabaseName(databaseName != Constants.SystemDatabase ? databaseName : null);

            checkPorts = true;
            if (dataDirectory != null)
                pathsToDelete.Add(dataDirectory);

            var storageType = GetDefaultStorageType(requestedStorage);
            var directory = dataDirectory ?? NewDataPath(databaseName == Constants.SystemDatabase ? null : databaseName);
            var ravenConfiguration = new RavenConfiguration();

            ConfigurationHelper.ApplySettingsToConfiguration(ravenConfiguration);

            ravenConfiguration.Port = port;
            ravenConfiguration.DataDirectory = Path.Combine(directory, "System");
            ravenConfiguration.RunInMemory = runInMemory;
#if DEBUG
            ravenConfiguration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = runInMemory;
#endif
            ravenConfiguration.DefaultStorageTypeName = storageType;
            ravenConfiguration.AnonymousUserAccessMode = enableAuthentication ? AnonymousUserAccessMode.None : AnonymousUserAccessMode.Admin;

            ravenConfiguration.FileSystem.DataDirectory = Path.Combine(directory, "FileSystem");
            ravenConfiguration.Encryption.UseFips = ConfigurationHelper.UseFipsEncryptionAlgorithms;

            ravenConfiguration.Settings["Raven/StorageTypeName"] = ravenConfiguration.DefaultStorageTypeName;

            if (activeBundles != null)
            {
                ravenConfiguration.Settings["Raven/ActiveBundles"] = activeBundles;
            }

            if (configureConfig != null)
                configureConfig(ravenConfiguration);
            ModifyConfiguration(ravenConfiguration);

            ravenConfiguration.PostInit();

            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port);
            var ravenDbServer = new RavenDbServer(ravenConfiguration)
            {
                UseEmbeddedHttpServer = true,
            };
            ravenDbServer.Initialize(configureServer);
            servers.Add(ravenDbServer);

            try
            {
                using (var documentStore = new DocumentStore
                {
                    Url = "http://localhost:" + port,
                    Conventions =
                    {
                        FailoverBehavior = FailoverBehavior.FailImmediately
                    },
                    DefaultDatabase = databaseName
                }.Initialize())
                {
                    CreateDefaultIndexes(documentStore);
                }
            }
            catch
            {
                ravenDbServer.Dispose();
                throw;
            }

            if (enableAuthentication)
            {
                EnableAuthentication(ravenDbServer.SystemDatabase);
                ModifyConfiguration(ravenConfiguration);
                ravenConfiguration.PostInit();
            }

            return ravenDbServer;
        }

        public ITransactionalStorage NewTransactionalStorage(string requestedStorage = null, string dataDir = null, string tempDir = null, bool? runInMemory = null, OrderedPartCollection<AbstractDocumentCodec> documentCodecs = null, Action onCommit = null)
        {
            ITransactionalStorage newTransactionalStorage;
            string storageType = GetDefaultStorageType(requestedStorage);

            var dataDirectory = dataDir ?? NewDataPath();
            var ravenConfiguration = new RavenConfiguration
            {
                DataDirectory = dataDirectory,
                RunInMemory = storageType.Equals("esent", StringComparison.OrdinalIgnoreCase) == false && (runInMemory ?? true),
            };

            ravenConfiguration.FileSystem.DataDirectory = Path.Combine(dataDirectory, "FileSystem");
            ravenConfiguration.Storage.Voron.TempPath = tempDir;

            Action onCommitNotification = () =>
            {
                if (onCommit != null)
                    onCommit();
            };

            if (storageType == "voron")
                newTransactionalStorage = new Raven.Storage.Voron.TransactionalStorage(ravenConfiguration, onCommitNotification, () => { }, () => { }, () => { });
            else
                newTransactionalStorage = new Raven.Storage.Esent.TransactionalStorage(ravenConfiguration, onCommitNotification, () => { }, () => { }, () => { });

            newTransactionalStorage.Initialize(new SequentialUuidGenerator { EtagBase = 0 }, documentCodecs ?? new OrderedPartCollection<AbstractDocumentCodec>());
            return newTransactionalStorage;
        }

        protected virtual void ModifyStore(DocumentStore documentStore)
        {
        }

        protected virtual void ModifyStore(EmbeddableDocumentStore documentStore)
        {
        }

        protected virtual void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
        }

        protected virtual void ModifyServer(RavenDbServer ravenDbServer)
        {
        }

        protected virtual void CreateDefaultIndexes(IDocumentStore documentStore)
        {
            new RavenDocumentsByEntityName().Execute(documentStore.DatabaseCommands, documentStore.Conventions);
        }

        /// <summary>
        /// The current process is paused until all the indexes stop being stale.
        /// </summary>
        /// <param name="store">The document store where the indexes exist.</param>
        /// <param name="database">The name of the database where the commands exist.</param>
        /// <param name="timeout">The maximum time you'll wait for the indexes to complete.</param>
        public static void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
        {
            var databaseCommands = store.DatabaseCommands;
            if (database != null)
            {
                databaseCommands = databaseCommands.ForDatabase(database);
            }
            
            timeout = timeout ?? (Debugger.IsAttached
                ? TimeSpan.FromMinutes(5)
                : TimeSpan.FromSeconds(20));

            if (databaseCommands.GetStatistics().Indexes.Length == 0)
                throw new Exception("Looks like you WaitForIndexing on database without indexes!");

            var spinUntil = SpinWait.SpinUntil(() => 
                databaseCommands.GetStatistics().CountOfStaleIndexesExcludingDisabledAndAbandoned == 0, 
                timeout.Value);
            if (spinUntil)
            {
                return;
            }

            var statistics = databaseCommands.GetStatistics();
            var stats = RavenJObject.FromObject(statistics).ToString(Formatting.Indented);
            var file = Path.GetTempFileName() + ".json";
            File.WriteAllText(file, stats);
            var errorMessage = string.Format("The indexes stayed stale for more than {0},{1}Details at: {2}",
                timeout.Value,
                Environment.NewLine,
                file);
            throw new TimeoutException(errorMessage);
        }

        public void WaitForPeriodicExport(DocumentDatabase db, PeriodicExportStatus previousStatus, Func<PeriodicExportStatus, Etag> compareSelector)
        {
            PeriodicExportStatus currentStatus = null;
            var done = SpinWait.SpinUntil(() =>
            {
                currentStatus = GetPeriodicBackupStatus(db);
                return compareSelector(currentStatus) != compareSelector(previousStatus);
            }, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
            if (!done) throw new Exception("WaitForPeriodicExport failed");
            previousStatus.LastDocsEtag = currentStatus.LastDocsEtag;
            previousStatus.LastAttachmentsEtag = currentStatus.LastAttachmentsEtag;
            previousStatus.LastDocsDeletionEtag = currentStatus.LastDocsDeletionEtag;
            previousStatus.LastAttachmentDeletionEtag = currentStatus.LastAttachmentDeletionEtag;

        }

        /// <summary>
        /// The current process is paused until all the indexes stop being stale.
        /// </summary>
        /// <param name="db">The document database where the indexes exist.</param>
        public static void WaitForIndexing(DocumentDatabase db)
        {
            if (db.Statistics.Indexes.Length == 0) 
                throw new Exception("Looks like you WaitForIndexing on database without indexes!");

            if (!SpinWait.SpinUntil(() => db.Statistics.StaleIndexes.Length == 0, TimeSpan.FromMinutes(5)))
                throw new Exception("WaitForIndexing failed");
        }

        public static void WaitForAllRequestsToComplete(RavenDbServer server)
        {
            if (!SpinWait.SpinUntil(() => server.Server.HasPendingRequests == false, TimeSpan.FromMinutes(15)))
            {
                throw new Exception("WaitForAllRequestsToComplete failed");
            }
        }

        protected PeriodicExportStatus GetPeriodicBackupStatus(DocumentDatabase db)
        {
            return GetPerodicBackupStatus(key => db.Documents.Get(key, null));
        }

        protected PeriodicExportStatus GetPerodicBackupStatus(IDatabaseCommands commands)
        {
            return GetPerodicBackupStatus(commands.Get);
        }

        private PeriodicExportStatus GetPerodicBackupStatus(Func<string, JsonDocument> getDocument)
        {
            var jsonDocument = getDocument(PeriodicExportStatus.RavenDocumentKey);
            if (jsonDocument == null)
                return new PeriodicExportStatus();

            return jsonDocument.DataAsJson.JsonDeserialization<PeriodicExportStatus>();
        }

        protected void WaitForPeriodicExport(DocumentDatabase db, PeriodicExportStatus previousStatus,
            PeriodicExportStatus.PeriodicExportStatusEtags statusEtags = PeriodicExportStatus.PeriodicExportStatusEtags.All)
        {
            WaitForPeriodicExport(key => db.Documents.Get(key, null), previousStatus, statusEtags);
        }

        protected void WaitForPeriodicExport(IDatabaseCommands commands, PeriodicExportStatus previousStatus,
            PeriodicExportStatus.PeriodicExportStatusEtags statusEtags = PeriodicExportStatus.PeriodicExportStatusEtags.All)
        {
            WaitForPeriodicExport(commands.Get, previousStatus, statusEtags);
        }

        private void WaitForPeriodicExport(Func<string, JsonDocument> getDocument, PeriodicExportStatus previousStatus, 
            PeriodicExportStatus.PeriodicExportStatusEtags statusEtags = PeriodicExportStatus.PeriodicExportStatusEtags.All)
        {
            PeriodicExportStatus currentStatus = null;
            var done = SpinWait.SpinUntil(() =>
            {
                currentStatus = GetPerodicBackupStatus(getDocument);
                return  (statusEtags.HasFlag(PeriodicExportStatus.PeriodicExportStatusEtags.LastDocsEtag) && currentStatus.LastDocsEtag != previousStatus.LastDocsEtag) ||
                       (statusEtags.HasFlag(PeriodicExportStatus.PeriodicExportStatusEtags.LastAttachmentsEtag) && currentStatus.LastAttachmentsEtag != previousStatus.LastAttachmentsEtag) ||
                       (statusEtags.HasFlag(PeriodicExportStatus.PeriodicExportStatusEtags.LastDocsDeletionEtag) && currentStatus.LastDocsDeletionEtag != previousStatus.LastDocsDeletionEtag) ||
                       (statusEtags.HasFlag(PeriodicExportStatus.PeriodicExportStatusEtags.LastAttachmentDeletionEtag) && currentStatus.LastAttachmentDeletionEtag != previousStatus.LastAttachmentDeletionEtag);

            }, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
            if (!done) 
                throw new Exception("WaitForPeriodicExport failed");

            previousStatus.LastDocsEtag = currentStatus.LastDocsEtag;
            previousStatus.LastAttachmentsEtag = currentStatus.LastAttachmentsEtag;
            previousStatus.LastDocsDeletionEtag = currentStatus.LastDocsDeletionEtag;
            previousStatus.LastAttachmentDeletionEtag = currentStatus.LastAttachmentDeletionEtag;

        }

        protected void WaitForBackup(DocumentDatabase db, bool checkError)
        {
            WaitForBackup(key => db.Documents.Get(key, null), checkError);
        }

        private void WaitForBackup(Func<string, JsonDocument> getDocument, bool checkError)
        {
            var done = SpinWait.SpinUntil(() =>
            {
                // We expect to get the doc from database that we tried to backup
                var jsonDocument = getDocument(BackupStatus.RavenBackupStatusDocumentKey);
                if (jsonDocument == null)
                    return true;

                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                if (backupStatus.IsRunning == false)
                {
                    if (checkError)
                    {
                        var firstOrDefault =
                            backupStatus.Messages.FirstOrDefault(x => x.Severity == BackupStatus.BackupMessageSeverity.Error);
                        if (firstOrDefault != null)
                            throw new Exception(string.Format("{0}\n\nDetails: {1}", firstOrDefault.Message, firstOrDefault.Details));
                    }

                    return true;
                }
                return false;
            }, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
            if (!done) throw new Exception("WaitForBackup failed");
        }

        protected void WaitForRestore(IDatabaseCommands databaseCommands)
        {
            var systemDatabaseCommands = databaseCommands.ForSystemDatabase(); // need to be sure that we are checking system database

            var failureMessages = new[]
            {
                                      "Esent Restore: Failure! Could not restore database!", 
                                      "Error: Restore Canceled", 
                                      "Restore Operation: Failure! Could not restore database!"
                                  };

            var restoreFinishMessages = new[]
                {
                    "The new database was created",
                    "Esent Restore: Restore Complete", 
                    "Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name",
                };

            var done = SpinWait.SpinUntil(() =>
            {
                // We expect to get the doc from the <system> database
                var doc = systemDatabaseCommands.Get(RestoreStatus.RavenRestoreStatusDocumentKey);

                if (doc == null)
                    return false;

                var status = doc.DataAsJson.Deserialize<RestoreStatus>(new DocumentConvention());

                if (failureMessages.Any(status.Messages.Contains))
                    throw new InvalidOperationException("Restore failure: " + status.Messages.Aggregate(string.Empty, (output, message) => output + (message + Environment.NewLine)));

                return restoreFinishMessages.Any(status.Messages.Contains);
            }, TimeSpan.FromMinutes(5));

            if (!done) throw new Exception("WaitForRestore failed");
        }

        protected virtual void WaitForDocument(IDatabaseCommands databaseCommands, string id, Etag afterEtag = null)
        {
            WaitForDocument(databaseCommands, id, TimeSpan.FromMinutes(5), afterEtag);
        }

        protected virtual void WaitForDocument(IDatabaseCommands databaseCommands, string id, TimeSpan timeout, Etag afterEtag = null)
        {
            var done = SpinWait.SpinUntil(() =>
            {
                // We expect to get the doc from the <system> database
                var doc = databaseCommands.Get(id);
                if (afterEtag == null)
                    return doc != null; 
                return EtagUtil.IsGreaterThan(doc.Etag, afterEtag);
            }, timeout);

            if (!done) throw new Exception("WaitForDocument failed");
        }

        protected static void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            var databaseName = Constants.SystemDatabase;

            var embeddableDocumentStore = documentStore as EmbeddableDocumentStore;
            OwinHttpServer server = null;
            string url = documentStore.Url;
            if (embeddableDocumentStore != null)
            {
                databaseName = embeddableDocumentStore.DefaultDatabase;
                embeddableDocumentStore.Configuration.Port = port;
                SetStudioConfigToAllowSingleDb(embeddableDocumentStore);
                embeddableDocumentStore.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.Admin;
                NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
                server = new OwinHttpServer(embeddableDocumentStore.Configuration, embeddableDocumentStore.DocumentDatabase);
                url = embeddableDocumentStore.Configuration.ServerUrl;
            }

            var remoteDocumentStore = documentStore as DocumentStore;
            if (remoteDocumentStore != null)
            {
                databaseName = remoteDocumentStore.DefaultDatabase;
            }

            using (server)
            {
                try
                {
                    var databaseNameEncoded = Uri.EscapeDataString(databaseName ?? Constants.SystemDatabase);
                    var documentsPage = url + "studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";
                    Process.Start(documentsPage); // start the server
                }
                catch (Win32Exception e)
                {
                    Console.WriteLine("Failed to open the browser. Please open it manually at {0}. {1}", url, e);
                }

                do
                {
                    Thread.Sleep(100);
                } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
            }
        }

        /// <summary>
        ///     Let the studio knows that it shouldn't display the warning about sys db access
        /// </summary>
        public static void SetStudioConfigToAllowSingleDb(IDocumentStore documentDatabase)
        {
            JsonDocument jsonDocument = documentDatabase.DatabaseCommands.Get("Raven/StudioConfig");
            RavenJObject doc;
            RavenJObject metadata;
            if (jsonDocument == null)
            {
                doc = new RavenJObject();
                metadata = new RavenJObject();
            }
            else
            {
                doc = jsonDocument.DataAsJson;
                metadata = jsonDocument.Metadata;
            }

            doc["WarnWhenUsingSystemDatabase"] = false;

            documentDatabase.DatabaseCommands.Put("Raven/StudioConfig", null, doc, metadata);
        }

    /*    private void isServerExsist(string url)
        {
            checkPorts.CompareTo(url);

        }*/

        protected void WaitForUserToContinueTheTest(bool debug = true, string url = null, string startPage = null)
        {
            if (debug && Debugger.IsAttached == false)
                return;


            using (var documentStore = new DocumentStore
            {
                Url = url ?? "http://localhost:8079"
            }.Initialize())
            {
                var databaseNameEncoded = Uri.EscapeDataString(Constants.SystemDatabase);
                var documentsPage = documentStore.Url + (startPage ?? "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true");
                var request = WebRequest.Create(documentsPage);

                try
                {
                    var response = request.GetResponse();
                }
                catch (WebException ex)
                {
                    
                    throw new NotSupportedException("when using a local store WaitForUserToContinueTheTest must be called with store parameter",ex);
                }

                Process.Start(documentsPage); // start the server

                do
                {
                    Thread.Sleep(100);
                } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
            }
        }

        protected void ClearDatabaseDirectory(string dataDir)
        {
            bool isRetry = false;

            while (true)
            {
                try
                {
                    IOExtensions.DeleteDirectory(dataDir);
                    break;
                }
                catch (IOException)
                {
                    if (isRetry)
                        throw;

                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    isRetry = true;

                    Thread.Sleep(2500);
                }
            }
        }

        public virtual void Dispose()
        {
            Authentication.Disable();
            GC.SuppressFinalize(this);

            var errors = new List<Exception>();
            //Disposing of the raft engine first otherwise raft will go crazy
            //during the dispose process
            foreach (var server in servers)
            {
                server.Options.ClusterManager.Value?.Dispose();
            }
            foreach (var store in stores)
            {
                try
                {
                    store.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }
            stores.Clear();

            foreach (var server in servers)
            {
                if (server == null)
                    continue;
                try
                {
                    server.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
            }

            servers.Clear();

            GC.Collect(2);
            GC.WaitForPendingFinalizers();

            foreach (var pathToDelete in pathsToDelete)
            {
                try
                {
                    ClearDatabaseDirectory(pathToDelete);
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
                finally
                {
                    if (File.Exists(pathToDelete)) // Just in order to be sure we didn't created a file in that path, by mistake)
                    {
                        errors.Add(new IOException(string.Format("We tried to delete the '{0}' directory, but failed because it is a file.\r\n{1}", pathToDelete,
                            WhoIsLocking.ThisFile(pathToDelete))));
                    }
                    else if (Directory.Exists(pathToDelete))
                    {
                        string filePath;
                        try
                        {
                            filePath = Directory.GetFiles(pathToDelete, "*", SearchOption.AllDirectories).FirstOrDefault() ?? pathToDelete;
                        }
                        catch (Exception)
                        {
                            filePath = pathToDelete;
                        }
                        errors.Add(new IOException(string.Format("We tried to delete the '{0}' directory.\r\n{1}", pathToDelete,
                            WhoIsLocking.ThisFile(filePath))));
                    }
                }
            }

            if (errors.Count > 0)
                throw new AggregateException(errors);
        }

        protected static void PrintServerErrors(IndexingError[] indexingErrors)
        {
            if (indexingErrors.Any())
            {
                Console.WriteLine("Server errors count: " + indexingErrors.Count());
                foreach (var serverError in indexingErrors)
                {
                    Console.WriteLine("Server error: " + serverError.ToString());
                }
            }
            else
                Console.WriteLine("No server errors");
        }

        protected void AssertNoIndexErrors(IDocumentStore documentStore)
        {
            var embeddableDocumentStore = documentStore as EmbeddableDocumentStore;
            var errors = embeddableDocumentStore != null
                                       ? embeddableDocumentStore.SystemDatabase.Statistics.Errors
                                       : documentStore.DatabaseCommands.GetStatistics().Errors;

            try
            {
                if (errors.Any()) throw new Exception("AssertNoIndexErrors Failed");
            }
            catch (Exception)
            {
                Console.WriteLine(errors.First().Error);
                throw;
            }
        }

        public static LicensingStatus GetLicenseByReflection(DocumentDatabase database)
        {
            var field = database.GetType().GetField("initializer", BindingFlags.Instance | BindingFlags.NonPublic);
            if (null == field) throw new Exception("LicensingStatus failed");
            var initializer = field.GetValue(database);
            var validateLicenseField = initializer.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
            if (null == validateLicenseField) throw new Exception("LicensingStatus failed");
            var validateLicense = validateLicenseField.GetValue(initializer);

            var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);
            if (null == currentLicenseProp) throw new Exception("LicensingStatus failed");

            return (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);
        }

        protected string NormalizeDatabaseName(string databaseName)
        {
            if (string.IsNullOrEmpty(databaseName))
            {
                return null;
            }

            if (databaseName.Length < 50)
            {
                DatabaseNames.Add(databaseName);
                return databaseName;
            }

            var prefix = databaseName.Substring(0, 30);
            var suffix = databaseName.Substring(databaseName.Length - 10, 10);
            var hash = new Guid(Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(databaseName))).ToString("N").Substring(0, 8);

            var name = string.Format("{0}_{1}_{2}", prefix, hash, suffix);

            DatabaseNames.Add(name);

            return name;
        }

        protected static void DeployNorthwind(DocumentStore store, string databaseName = null)
        {
            if (string.IsNullOrEmpty(databaseName) == false)
                store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(databaseName);

            var url = store.Url.ForDatabase(string.IsNullOrEmpty(databaseName) == false ? databaseName : store.DefaultDatabase);

            var requestFactory = store.JsonRequestFactory;
            var request = requestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url + "/studio-tasks/createSampleData", HttpMethods.Post, store.DatabaseCommands.PrimaryCredentials, store.Conventions));
            request.ExecuteRequest();
        }

        private static void StoreSeedData(IEnumerable<IEnumerable> seedData,
            IDocumentStore documentStore)
        {
            if (seedData == null)
            {
                throw new ArgumentNullException("seedData");
            }

            using (var session = documentStore.OpenSession())
            {
                foreach (var collection in seedData)
                {
                    foreach (var item in collection)
                    {
                        session.Store(item);
                    }
                }

                session.SaveChanges();
            }
        }

        private static void ExecuteIndexes(IEnumerable<AbstractIndexCreationTask> indexes,
            IDocumentStore documentStore)
        {
            if (indexes == null)
            {
                throw new ArgumentNullException("indexes");
            }

            if (documentStore == null)
            {
                throw new ArgumentNullException("documentStore");
            }

            foreach (var index in indexes)
            {
                index.Execute(documentStore);
            }
        }

        private static void ExecuteTransformers(IEnumerable<AbstractTransformerCreationTask> transformers,
            IDocumentStore documentStore)
        {
            if (transformers == null)
            {
                throw new ArgumentNullException("transformers");
            }

            if (documentStore == null)
            {
                throw new ArgumentNullException("documentStore");
            }

            foreach (var transformer in transformers)
            {
                transformer.Execute(documentStore);
            }
        }

        public static IEnumerable<object[]> InsertOptions
        {
            get
            {
                yield return new[] { new BulkInsertOptions { Format = BulkInsertFormat.Bson, Compression = BulkInsertCompression.GZip } };
                yield return new[] { new BulkInsertOptions { Format = BulkInsertFormat.Json } };
                yield return new[] { new BulkInsertOptions { Compression = BulkInsertCompression.None } };
    }
}

        protected RavenDbServer CreateServerWithWindowsCredentials(int port, string username, string password, string domain, out NodeConnectionInfo nodeConnectionInfo)
        {
            var server = GetNewServer(port, enableAuthentication: true);
            nodeConnectionInfo = ClusterManagerFactory.CreateSelfConnection(server.SystemDatabase);
            nodeConnectionInfo.Username = username;
            nodeConnectionInfo.Password = password;
            nodeConnectionInfo.Domain = domain;

            EnableAuthentication(server.SystemDatabase);
            NewRemoteDocumentStore(ravenDbServer: server);
            return server;
        }

        protected RavenDbServer CreateServerWithOAuth(int port, string apiKey, out NodeConnectionInfo nodeConnectionInfo)
        {
            var server = GetNewServer(port, enableAuthentication: true);
            nodeConnectionInfo = ClusterManagerFactory.CreateSelfConnection(server.SystemDatabase);
            nodeConnectionInfo.ApiKey = apiKey;

            EnableAuthentication(server.SystemDatabase);

            var apiKeyTokens = apiKey.Split('/');

            server.SystemDatabase.Documents.Put("Raven/ApiKeys/" + apiKeyTokens[0], null, RavenJObject.FromObject(new ApiKeyDefinition
            {
                Databases = new List<ResourceAccess>
                                    {
                                        new ResourceAccess { TenantId = "*", Admin = true }, 
                                        new ResourceAccess { TenantId = "<system>", Admin = true },
                                    },
                Enabled = true,
                Name = apiKeyTokens[0],
                Secret = apiKeyTokens[1]
            }), new RavenJObject(), null);

            NewRemoteDocumentStore(ravenDbServer: server);

            return server;
        }

        protected IDisposable WithCustomDatabaseSettings(Action<DatabaseDocument> settingConfigurator)
        {
            MultiDatabase.ConfigureDatabaseDocument = settingConfigurator;

            return new DisposableAction(() => MultiDatabase.ConfigureDatabaseDocument = null);
        }

        protected static int GetCachedItemsCount(DocumentStore store)
        {
            var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null,
                    store.Url + $"/databases/{store.DefaultDatabase}/debug/cache-details",
                    HttpMethod.Get,
                    store.DatabaseCommands.PrimaryCredentials,
                    store.Conventions));

            var response = request.ReadResponseJson();
            return response.Value<int>("CachedItems");
        }
    }
}
