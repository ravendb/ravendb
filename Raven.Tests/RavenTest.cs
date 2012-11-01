//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using NLog;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests
{
	public class RavenTest : WithNLog, IDisposable
	{
		protected const string DbDirectory = @".\TestDb\";
		protected const string DbName = DbDirectory + @"DocDb.esb";
		private string path;
		private readonly List<IDocumentStore> stores = new List<IDocumentStore>();

		static RavenTest()
		{
			File.Delete("test.log");
		}

		protected void Consume(object o)
		{
			
		}

		public EmbeddableDocumentStore NewDocumentStore(
			bool deleteDirectory = true,
			string requestedStorage = null,
			ComposablePartCatalog catalog = null,
			bool deleteDirectoryOnDispose = true)
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

			string defaultStorageType = GetDefaultStorageType(requestedStorage);

			var documentStore = new EmbeddableDocumentStore
			{
				Configuration =
				{
					DefaultStorageTypeName = defaultStorageType,
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					RunInMemory = false,
					Port = 8079
				}
			};

			if (catalog != null)
				documentStore.Configuration.Catalog.Catalogs.Add(catalog);

			try
			{
				ModifyStore(documentStore);
				ModifyConfiguration(documentStore.Configuration);

				if (deleteDirectory)
					IOExtensions.DeleteDirectory(path);

				documentStore.Initialize();

				CreateDefaultIndexes(documentStore);

				if (deleteDirectoryOnDispose)
					documentStore.Disposed += ClearDatabaseDirectory;

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

		public static string GetDefaultStorageType(string requestedStorage = null)
		{
			string defaultStorageType;
			var envVar = Environment.GetEnvironmentVariable("raventest_storage_engine");
			if (string.IsNullOrEmpty(envVar) == false)
				defaultStorageType = envVar;
			else if (requestedStorage != null)
				defaultStorageType = requestedStorage;
			else
				defaultStorageType = "munin";
			return defaultStorageType;
		}

		public ITransactionalStorage NewTransactionalStorage()
		{
			ITransactionalStorage newTransactionalStorage;
			string storageType = null;

			if (!string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("raventest_storage_engine")))
				storageType = System.Environment.GetEnvironmentVariable("raventest_storage_engine");
			else
				storageType = System.Configuration.ConfigurationManager.AppSettings["Raven/StorageEngine"];

			if (storageType == "munin")
				newTransactionalStorage = new Raven.Storage.Managed.TransactionalStorage(new RavenConfiguration { DataDirectory = DbDirectory, }, () => { });
			else
				newTransactionalStorage = new Raven.Storage.Esent.TransactionalStorage(new RavenConfiguration { DataDirectory = DbDirectory, }, () => { });

			newTransactionalStorage.Initialize(new DummyUuidGenerator(), new OrderedPartCollection<AbstractDocumentCodec>());
			return newTransactionalStorage;
		}

		protected void WaitForBackup(DocumentDatabase db, bool checkError)
		{
			while (true)
			{
				var jsonDocument = db.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
				if (jsonDocument == null)
					break;

				var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
				if (backupStatus.IsRunning == false)
				{
					if (checkError)
					{
						var firstOrDefault = backupStatus.Messages.FirstOrDefault(x => x.Severity == BackupStatus.BackupMessageSeverity.Error);
						if (firstOrDefault != null)
							Assert.False(true, firstOrDefault.Message);
					}

					return;
				}
				Thread.Sleep(50);
			}
		}

		protected virtual void CreateDefaultIndexes(IDocumentStore documentStore)
		{
			new RavenDocumentsByEntityName().Execute(documentStore);
		}

		protected virtual void ModifyStore(EmbeddableDocumentStore documentStore)
		{

		}

		protected virtual void ModifyStore(DocumentStore documentStore)
		{

		}

		static public void WaitForUserToContinueTheTest(EmbeddableDocumentStore documentStore, bool debug = true)
		{
			if (debug && Debugger.IsAttached == false)
				return;

			documentStore.SetStudioConfigToAllowSingleDb();

			documentStore.DatabaseCommands.Put("Pls Delete Me", null,

											   RavenJObject.FromObject(new { StackTrace = new StackTrace(true) }),
											   new RavenJObject());

			documentStore.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.All;
			using (var server = new HttpServer(documentStore.Configuration, documentStore.DocumentDatabase))
			{
				server.StartListening();
				Process.Start(documentStore.Configuration.ServerUrl); // start the server

				do
				{
					Thread.Sleep(100);
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null && (debug == false || Debugger.IsAttached));
			}
		}

		protected virtual void ModifyConfiguration(RavenConfiguration configuration)
		{
		}

		public static void WaitForIndexing(IDocumentStore store)
		{
			while (store.DatabaseCommands.GetStatistics().StaleIndexes.Length > 0)
			{
				Thread.Sleep(100);
			}
		}

		protected void WaitForAllRequestsToComplete(RavenDbServer server)
		{
			while (server.Server.HasPendingRequests)
				Thread.Sleep(25);
		}

		protected void WaitForUserToContinueTheTest()
		{
			if (Debugger.IsAttached == false)
				return;

			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			})
			{
				documentStore.Initialize();
				documentStore.DatabaseCommands.Put("Pls Delete Me", null,
												   RavenJObject.FromObject(new { StackTrace = new StackTrace(true) }), new RavenJObject());

				Process.Start(documentStore.Url);// start the server

				do
				{
					Thread.Sleep(100);
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null);
			}

		}

		protected RavenDbServer GetNewServer(int port = 8079, string dataDirectory = "Data", bool runInMemory = true)
		{
			var ravenConfiguration = new RavenConfiguration
									 {
										 Port = port,
										 DataDirectory = dataDirectory,
										 RunInMemory = runInMemory,
										 AnonymousUserAccessMode = AnonymousUserAccessMode.All
									 };

			ModifyConfiguration(ravenConfiguration);

			if (ravenConfiguration.RunInMemory == false)
				IOExtensions.DeleteDirectory(ravenConfiguration.DataDirectory);

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port);
			var ravenDbServer = new RavenDbServer(ravenConfiguration);

			try
			{
				using (var documentStore = new DocumentStore
											{
												Url = "http://localhost:" + port,
												Conventions =
													{
														FailoverBehavior = FailoverBehavior.FailImmediately
													},
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
			return ravenDbServer;
		}

		protected string GetPath(string subFolderName)
		{
			string retPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			return Path.Combine(retPath, subFolderName).Substring(6); //remove leading file://
		}

		public RavenTest()
		{
			DatabaseMemoryTarget databaseMemoryTarget = null;
			if (LogManager.Configuration != null && LogManager.Configuration.AllTargets != null)
			{
				databaseMemoryTarget = LogManager.Configuration.AllTargets.OfType<DatabaseMemoryTarget>().FirstOrDefault();
			}
			if (databaseMemoryTarget != null)
			{
				databaseMemoryTarget.ClearAll();
			}

			try
			{
				new Uri("http://fail/first/time?only=%2bplus");
			}
			catch (Exception)
			{
			}

			SystemTime.UtcDateTime = () => DateTime.UtcNow;

			ClearDatabaseDirectory();

			Directory.CreateDirectory(DbDirectory);
		}

		protected void ClearDatabaseDirectory()
		{
			bool isRetry = false;

			while (true)
			{
				try
				{
					IOExtensions.DeleteDirectory(DbName);
					IOExtensions.DeleteDirectory(DbDirectory);
					break;
				}
				catch (IOException)
				{
					if (isRetry)
						throw;

					GC.Collect();
					GC.WaitForPendingFinalizers();
					isRetry = true;
				}
			}
		}

		public double Timer(Action action)
		{
			var startTime = SystemTime.UtcNow;
			action.Invoke();
			var timeTaken = SystemTime.UtcNow.Subtract(startTime);
			Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
			return timeTaken.TotalMilliseconds;
		}

		public IDocumentStore NewRemoteDocumentStore(bool fiddler = false)
		{
			var ravenDbServer = GetNewServer();
			ModifyServer(ravenDbServer);
			var store = new DocumentStore
			{
				Url = fiddler ? "http://localhost.fiddler:8079" : "http://localhost:8079"
			};

			store.AfterDispose += (sender, args) =>
			{
				ravenDbServer.Dispose();
				ClearDatabaseDirectory();
			};
			ModifyStore(store);
			return store.Initialize();
		}

		protected virtual void ModifyServer(RavenDbServer ravenDbServer)
		{
		}

		public virtual void Dispose()
		{
			stores.Where(store => store != null).ForEach(store => store.Dispose());
			GC.Collect(2);
			GC.WaitForPendingFinalizers();
			ClearDatabaseDirectory();
		}
	}
}