//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Primitives;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
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
using Raven.Json.Linq;
using Raven.Server;
using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Helpers
{
	public class RavenTestBase : IDisposable
	{
		protected readonly string DataDir = string.Format(@".\TestDatabase-{0}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"));

		private string path;
		protected readonly List<IDocumentStore> stores = new List<IDocumentStore>();

		public RavenTestBase()
		{
			CommonInitializationUtil.Initialize();

			ClearDatabaseDirectory();
			Directory.CreateDirectory(DataDir);
		}

		public EmbeddableDocumentStore NewDocumentStore(
			bool runInMemory = true,
			string requestedStorage = null,
			ComposablePartCatalog catalog = null,
			bool deleteDirectory = true,
			bool deleteDirectoryOnDispose = true)
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(RavenTestBase)).CodeBase);
			path = Path.Combine(path, DataDir).Substring(6);

			var storageType = GetDefaultStorageType(requestedStorage);
			var documentStore = new EmbeddableDocumentStore
			{
				Configuration =
				{
					DefaultStorageTypeName = storageType,
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					RunInMemory = storageType.Equals("esent", StringComparison.OrdinalIgnoreCase) == false && runInMemory,
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

		public IDocumentStore NewRemoteDocumentStore(bool fiddler = false, RavenDbServer ravenDbServer = null, string databaseName = null,
			 bool deleteDirectoryAfter = true, 
			 bool deleteDirectoryBefore = true,
			 bool runInMemory = true)
		{
			ravenDbServer = ravenDbServer ?? GetNewServer(runInMemory: runInMemory, deleteDirectory: deleteDirectoryBefore);
			ModifyServer(ravenDbServer);
			var store = new DocumentStore
			{
				Url = GetServerUrl(fiddler),
				DefaultDatabase = databaseName,
			};
			store.AfterDispose += (sender, args) =>
			{
				ravenDbServer.Dispose();
				if (deleteDirectoryAfter)
					ClearDatabaseDirectory();
			};
			ModifyStore(store);
			return store.Initialize();
		}

		private static string GetServerUrl(bool fiddler)
		{
			if (fiddler)
			{
				if (Process.GetProcessesByName("fiddler").Any())
					return "http://localhost.fiddler:8079";
			}
			return "http://localhost:8079";
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

		protected RavenDbServer GetNewServer(int port = 8079, string dataDirectory = "Data", bool runInMemory = true, bool deleteDirectory = true)
		{
			var ravenConfiguration = new RavenConfiguration
			{
				Port = port,
				DataDirectory = dataDirectory,
				RunInMemory = runInMemory,
				AnonymousUserAccessMode = AnonymousUserAccessMode.Admin
			};

			ModifyConfiguration(ravenConfiguration);

			ravenConfiguration.PostInit();

			if (ravenConfiguration.RunInMemory == false && deleteDirectory)
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

		public ITransactionalStorage NewTransactionalStorage(string requestedStorage = null)
		{
			ITransactionalStorage newTransactionalStorage;
			string storageType = GetDefaultStorageType(requestedStorage);

			if (storageType == "munin")
				newTransactionalStorage = new Storage.Managed.TransactionalStorage(new RavenConfiguration { DataDirectory = DataDir, }, () => { });
			else
				newTransactionalStorage = new Storage.Esent.TransactionalStorage(new RavenConfiguration { DataDirectory = DataDir, }, () => { });

			newTransactionalStorage.Initialize(new DummyUuidGenerator(), new OrderedPartCollection<AbstractDocumentCodec>());
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
			new RavenDocumentsByEntityName().Execute(documentStore);
		}

		public static void WaitForIndexing(IDocumentStore store, string db = null)
		{
			var databaseCommands = store.DatabaseCommands;
			if (db != null)
				databaseCommands = databaseCommands.ForDatabase(db);
			SpinWait.SpinUntil(() => databaseCommands.GetStatistics().StaleIndexes.Length == 0);
		}

		public static void WaitForAllRequestsToComplete(RavenDbServer server)
		{
			while (server.Server.HasPendingRequests)
			{
				Thread.Sleep(25);
			}
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

		public static void WaitForUserToContinueTheTest(EmbeddableDocumentStore documentStore, bool debug = true)
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

		protected void WaitForUserToContinueTheTest(bool debug = true)
		{
			if (debug && Debugger.IsAttached == false)
				return;

			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			})
			{
				documentStore.Initialize();
				documentStore.DatabaseCommands.Put("Pls Delete Me", null,
												   RavenJObject.FromObject(new { StackTrace = new StackTrace(true) }), new RavenJObject());

				Process.Start(documentStore.Url); // start the server

				do
				{
					Thread.Sleep(100);
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null && (debug == false || Debugger.IsAttached));
			}
		}

		protected void ClearDatabaseDirectory()
		{
			bool isRetry = false;

			while (true)
			{
				try
				{
					IOExtensions.DeleteDirectory(DataDir);
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

		public virtual void Dispose()
		{
			stores.Where(store => store != null).ForEach(store => store.Dispose());
			GC.Collect(2);
			GC.WaitForPendingFinalizers();
			ClearDatabaseDirectory();
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
	}
}