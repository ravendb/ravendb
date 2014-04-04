//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Primitives;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Encryptors;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS.Util;
using Raven.Database.Server.Security;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Common.Util;

using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Common
{
	public abstract class RavenTestBase : IDisposable
	{
		protected readonly List<RavenDbServer> servers = new List<RavenDbServer>();
		protected readonly List<IDocumentStore> stores = new List<IDocumentStore>();
		private readonly HashSet<string> pathsToDelete = new HashSet<string>();

		private static int pathCount;

		protected RavenTestBase()
		{
            Environment.SetEnvironmentVariable(Constants.RavenDefaultQueryTimeout, "30");
			CommonInitializationUtil.Initialize();

			// Make sure to delete the Data folder which we be used by tests that do not call the NewDataPath from whatever reason.
			var dataFolder = FilePathTools.MakeSureEndsWithSlash(@"~\Data".ToFullPath());
			ClearDatabaseDirectory(dataFolder);
			pathsToDelete.Add(dataFolder);
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
			if(prefix != null)
				prefix = prefix.Replace("<", "").Replace(">", "");

			var newDataDir = Path.GetFullPath(string.Format(@".\{1}-{0}-{2}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "TestDatabase", Interlocked.Increment(ref pathCount)));
		    if (forceCreateDir && Directory.Exists(newDataDir) == false)
		        Directory.CreateDirectory(newDataDir);
			pathsToDelete.Add(newDataDir);
			return newDataDir;
		}

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
            [CallerMemberName] string databaseName = null)
		{
		    databaseName = NormalizeDatabaseName(databaseName);

			var storageType = GetDefaultStorageType(requestedStorage);
			var dataDirectory = dataDir ?? NewDataPath(databaseName);
			var documentStore = new EmbeddableDocumentStore
			{
				UseEmbeddedHttpServer = port.HasValue,
				Configuration =
				{
					DefaultStorageTypeName = storageType,
					DataDirectory = dataDirectory,
					FileSystemDataDirectory = Path.Combine(dataDirectory, "FileSystem"),
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					RunInMemory = storageType.Equals("esent", StringComparison.OrdinalIgnoreCase) == false && runInMemory,
					Port = port == null ? 8079 : port.Value,
					UseFips = SettingsHelper.UseFipsEncryptionAlgorithms,
					AnonymousUserAccessMode = anonymousUserAccessMode,
				}
			};

			if (activeBundles != null)
			{
				documentStore.Configuration.Settings["Raven/ActiveBundles"] = activeBundles;
			}

			if (catalog != null)
				documentStore.Configuration.Catalog.Catalogs.Add(catalog);

			try
			{
				if (configureStore != null) 
					configureStore(documentStore);
				ModifyStore(documentStore);
				ModifyConfiguration(documentStore.Configuration);
                documentStore.Configuration.PostInit();
				documentStore.Initialize();

				if (enableAuthentication)
				{
					EnableAuthentication(documentStore.DocumentDatabase);
				}

				CreateDefaultIndexes(documentStore);

				return documentStore;
			}
			catch
			{
				// We must dispose of this object in exceptional cases, otherwise this test will break all the following tests.
				documentStore.Dispose();
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

			// rerun this startup task
			database.StartupTasks.OfType<AuthenticationForCommercialUseOnly>().First().Execute(database);
		}

		public DocumentStore NewRemoteDocumentStore(bool fiddler = false, RavenDbServer ravenDbServer = null, [CallerMemberName] string databaseName = null,
				bool runInMemory = true,
				string dataDirectory = null,
				string requestedStorage = null,
				bool enableAuthentication = false,
                bool ensureDatabaseExists = true,
				Action<DocumentStore> configureStore = null)
		{
		    databaseName = NormalizeDatabaseName(databaseName);

		    checkPorts = true;
			ravenDbServer = ravenDbServer ?? GetNewServer(runInMemory: runInMemory, dataDirectory: dataDirectory, requestedStorage: requestedStorage, enableAuthentication: enableAuthentication, databaseName: databaseName);
			ModifyServer(ravenDbServer);
			var store = new DocumentStore
			{
				Url = GetServerUrl(fiddler, ravenDbServer.SystemDatabase.ServerUrl),
				DefaultDatabase = databaseName				
			};
			stores.Add(store);
			store.AfterDispose += (sender, args) => ravenDbServer.Dispose();

			if (configureStore != null)
				configureStore(store);
			ModifyStore(store);

		    store.Initialize(ensureDatabaseExists);
			return store;
		}

		private static string GetServerUrl(bool fiddler, string serverUrl)
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
		    databaseName = NormalizeDatabaseName(databaseName != Constants.SystemDatabase ? databaseName : null);

		    checkPorts = true;
			if (dataDirectory != null)
				pathsToDelete.Add(dataDirectory);

			var storageType = GetDefaultStorageType(requestedStorage);
			var directory = dataDirectory ?? NewDataPath(databaseName == Constants.SystemDatabase ? null : databaseName);
			var ravenConfiguration = new RavenConfiguration
			{
				Port = port,
				DataDirectory = directory,
				FileSystemDataDirectory = Path.Combine(directory, "FileSystem"),
				RunInMemory = runInMemory,
#if DEBUG
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = runInMemory,
#endif
				DefaultStorageTypeName = storageType,
				AnonymousUserAccessMode = enableAuthentication ? AnonymousUserAccessMode.None : AnonymousUserAccessMode.Admin,
				UseFips = SettingsHelper.UseFipsEncryptionAlgorithms,
			};
			
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

		public ITransactionalStorage NewTransactionalStorage(string requestedStorage = null, string dataDir = null, string tempDir = null, bool? runInMemory = null, OrderedPartCollection<AbstractDocumentCodec> documentCodecs = null)
		{
			ITransactionalStorage newTransactionalStorage;
			string storageType = GetDefaultStorageType(requestedStorage);

			var dataDirectory = dataDir ?? NewDataPath();
			var ravenConfiguration = new RavenConfiguration
			{
				DataDirectory = dataDirectory,
				FileSystemDataDirectory = Path.Combine(dataDirectory, "FileSystem"),
				RunInMemory = storageType.Equals("esent", StringComparison.OrdinalIgnoreCase) == false && (runInMemory ?? true),
			};

            ravenConfiguration.Settings["Raven/Voron/TempPath"] = tempDir;

			if (storageType == "voron")
				newTransactionalStorage = new Raven.Storage.Voron.TransactionalStorage(ravenConfiguration, () => { });
			else
				newTransactionalStorage = new Raven.Storage.Esent.TransactionalStorage(ravenConfiguration, () => { });

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

		public static void WaitForIndexing(IDocumentStore store, string db = null,TimeSpan? timeout = null)
		{
			var databaseCommands = store.DatabaseCommands;
			if (db != null)
				databaseCommands = databaseCommands.ForDatabase(db);
		    bool spinUntil = SpinWait.SpinUntil(() => databaseCommands.GetStatistics().StaleIndexes.Length == 0, timeout ?? TimeSpan.FromSeconds(20));
		    if (spinUntil == false)
		        WaitForUserToContinueTheTest(store);
		    Assert.True(spinUntil, "Indexes took took long to become unstale");
		}

		public static void WaitForIndexing(DocumentDatabase db)
		{
			Assert.True(SpinWait.SpinUntil(() => db.Statistics.StaleIndexes.Length == 0, TimeSpan.FromMinutes(5)));
		}

		public static void WaitForAllRequestsToComplete(RavenDbServer server)
		{
			Assert.True(SpinWait.SpinUntil(() => server.Server.HasPendingRequests == false, TimeSpan.FromMinutes(15)));
		}

        protected PeriodicBackupStatus GetPerodicBackupStatus(DocumentDatabase db)
	    {
            return GetPerodicBackupStatus(key => db.Documents.Get(key, null));
	    }

        protected PeriodicBackupStatus GetPerodicBackupStatus(IDatabaseCommands commands)
        {
            return GetPerodicBackupStatus(commands.Get);
        }

        private PeriodicBackupStatus GetPerodicBackupStatus(Func<string, JsonDocument> getDocument)
        {
            var jsonDocument = getDocument(PeriodicBackupStatus.RavenDocumentKey);
            if (jsonDocument == null)
                return new PeriodicBackupStatus();

            return jsonDocument.DataAsJson.JsonDeserialization<PeriodicBackupStatus>();
        }

        protected void WaitForPeriodicBackup(DocumentDatabase db, PeriodicBackupStatus previousStatus)
        {
            WaitForPeriodicBackup(key => db.Documents.Get(key, null), previousStatus);
        }

        protected void WaitForPeriodicBackup(IDatabaseCommands commands, PeriodicBackupStatus previousStatus)
        {
            WaitForPeriodicBackup(commands.Get, previousStatus);
        }

        private void WaitForPeriodicBackup(Func<string, JsonDocument> getDocument, PeriodicBackupStatus previousStatus)
        {
            PeriodicBackupStatus currentStatus = null;
            var done = SpinWait.SpinUntil(() =>
            {
                currentStatus = GetPerodicBackupStatus(getDocument);
                return currentStatus.LastDocsEtag != previousStatus.LastDocsEtag ||
                       currentStatus.LastAttachmentsEtag != previousStatus.LastAttachmentsEtag ||
                       currentStatus.LastDocsDeletionEtag != previousStatus.LastDocsDeletionEtag ||
                       currentStatus.LastAttachmentDeletionEtag != previousStatus.LastAttachmentDeletionEtag;
            }, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
            Assert.True(done);
            previousStatus.LastDocsEtag = currentStatus.LastDocsEtag;
            previousStatus.LastAttachmentsEtag = currentStatus.LastAttachmentsEtag;
            previousStatus.LastDocsDeletionEtag = currentStatus.LastDocsDeletionEtag;
            previousStatus.LastAttachmentDeletionEtag = currentStatus.LastAttachmentDeletionEtag;

        }

		protected void WaitForBackup(DocumentDatabase db, bool checkError)
		{
			WaitForBackup(key => db.Documents.Get(key, null), checkError);
		}

		protected void WaitForBackup(IDatabaseCommands commands, bool checkError)
		{
			WaitForBackup(commands.Get, checkError);
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
							Assert.False(true, firstOrDefault.Message);
					}

					return true;
				}
				return false;
			}, Debugger.IsAttached ? TimeSpan.FromMinutes(120) : TimeSpan.FromMinutes(15));
			Assert.True(done);
		}

		protected void WaitForRestore(IDatabaseCommands databaseCommands)
		{
			var done = SpinWait.SpinUntil(() =>
			{
				// We expect to get the doc from the <system> database
				var doc = databaseCommands.Get(RestoreStatus.RavenRestoreStatusDocumentKey);

				if (doc == null)
					return false;

				var status = doc.DataAsJson.Deserialize<RestoreStatus>(new DocumentConvention());

				var restoreFinishMessages = new[]
				{
					"The new database was created",
					"Esent Restore: Restore Complete", 
					"Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name",
				};
				return restoreFinishMessages.Any(status.Messages.Contains);
			}, TimeSpan.FromMinutes(5));

			Assert.True(done);
		}

		protected void WaitForDocument(IDatabaseCommands databaseCommands, string id)
		{
			var done = SpinWait.SpinUntil(() =>
			{
				// We expect to get the doc from the <system> database
				var doc = databaseCommands.Get(id);
				return doc != null;
			}, TimeSpan.FromMinutes(5));

			Assert.True(done);
		}

		public static void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, int port = 8079)
		{
			if (debug && Debugger.IsAttached == false)
				return;

		    var embeddableDocumentStore = documentStore as EmbeddableDocumentStore;
			OwinHttpServer server = null;
            string url = documentStore.Url;
		    if (embeddableDocumentStore != null)
		    {
		        embeddableDocumentStore.Configuration.Port = port;
                SetStudioConfigToAllowSingleDb(embeddableDocumentStore);
                embeddableDocumentStore.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.Admin;
		        NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
                server = new OwinHttpServer(embeddableDocumentStore.Configuration, embeddableDocumentStore.DocumentDatabase);
                url = embeddableDocumentStore.Configuration.ServerUrl;
		    }

			using (server)
			{
				Process.Start(url); // start the server

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

		protected void WaitForUserToContinueTheTest(bool debug = true, string url = null)
		{
			if (debug && Debugger.IsAttached == false)
				return;

			using (var documentStore = new DocumentStore
			{
				Url = url ?? "http://localhost:8079"
			}.Initialize())
			{
			
				Process.Start(documentStore.Url); // start the server

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
			GC.SuppressFinalize(this);

			var errors = new List<Exception>();

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
					if (Directory.Exists(pathToDelete) || 
						File.Exists(pathToDelete)	// Just in order to be sure we didn't created a file in that path, by mistake
						)
					{
						errors.Add(new IOException(string.Format("We tried to delete the '{0}' directory, but failed", pathToDelete)));
					}
				}
			}

			if (errors.Count > 0)
				throw new AggregateException(errors);
		}

		protected static void PrintServerErrors(ServerError[] serverErrors)
		{
			if (serverErrors.Any())
			{
				Console.WriteLine("Server errors count: " + serverErrors.Count());
				foreach (var serverError in serverErrors)
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
									   ? embeddableDocumentStore.DocumentDatabase.Statistics.Errors
									   : documentStore.DatabaseCommands.GetStatistics().Errors;

			try
			{
				Assert.Empty(errors);
			}
			catch (EmptyException)
			{
				Console.WriteLine(errors.First().Error);
				throw;
			}
		}

		public static LicensingStatus GetLicenseByReflection(DocumentDatabase database)
		{
			var field = database.GetType().GetField("initializer", BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.NotNull(field);
			var initializer = field.GetValue(database);
			var validateLicenseField = initializer.GetType().GetField("validateLicense", BindingFlags.Instance | BindingFlags.NonPublic);
			Assert.NotNull(validateLicenseField);
			var validateLicense = validateLicenseField.GetValue(initializer);

			var currentLicenseProp = validateLicense.GetType().GetProperty("CurrentLicense", BindingFlags.Static | BindingFlags.Public);
			Assert.NotNull(currentLicenseProp);

			return (LicensingStatus)currentLicenseProp.GetValue(validateLicense, null);
		}

        protected string NormalizeDatabaseName(string databaseName)
        {
            if (string.IsNullOrEmpty(databaseName)) 
                return null;

            if (databaseName.Length < 50)
                return databaseName;

            var prefix = databaseName.Substring(0, 30);
            var suffix = databaseName.Substring(databaseName.Length - 10, 10);
            var hash = new Guid(Encryptor.Current.Hash.Compute16(Encoding.UTF8.GetBytes(databaseName))).ToString("N").Substring(0, 8);

            return string.Format("{0}_{1}_{2}", prefix, hash, suffix);
        }
	}
}
