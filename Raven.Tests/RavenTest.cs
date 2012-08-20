//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition.Hosting;
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

		static RavenTest()
		{
			File.Delete("test.log");
		}

		public EmbeddableDocumentStore NewDocumentStoreRestart()
		{
			return NewDocumentStoreInternal(deleteDirectory: false);
		}

		public EmbeddableDocumentStore NewDocumentStore()
		{
			return NewDocumentStoreInternal(deleteDirectory: true);
		}

		public EmbeddableDocumentStore NewDocumentStore(CompositionContainer container)
		{
			return NewDocumentStoreInternal(deleteDirectory: true, container: container);
		}

		public EmbeddableDocumentStore NewDocumentStore(AggregateCatalog catalog)
		{
			return NewDocumentStoreInternal(deleteDirectory: true, catalog: catalog);
		}

		private EmbeddableDocumentStore NewDocumentStoreInternal(bool deleteDirectory, CompositionContainer container = null, AggregateCatalog catalog = null)
		{
			string defaultStorageType = null;

			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof (DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

			if(!string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("raventest_storage_engine")))
				defaultStorageType = System.Environment.GetEnvironmentVariable("raventest_storage_engine");

			var documentStore = new EmbeddableDocumentStore
			{
				Configuration =
				{
					DefaultStorageTypeName = defaultStorageType,
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					RunInMemory = false,
					Container = container,
				}
			};

			if(catalog != null)
				documentStore.Configuration.Catalog = catalog;

			try
			{
				ModifyStore(documentStore);
				ModifyConfiguration(documentStore.Configuration);

				if(deleteDirectory)
					IOExtensions.DeleteDirectory(path);

				documentStore.Initialize();

				CreateDefaultIndexes(documentStore);

				return documentStore;
			}
			catch
			{
				// We must dispose of this object in exceptional cases, otherwise this test will break all the following tests.
				if (documentStore != null)
					documentStore.Dispose();
				throw;
			}
		}

		public ITransactionalStorage NewTransactionalStorage()
		{
			ITransactionalStorage newTransactionalStorage;
			string storageType = null;

			if(!string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("raventest_storage_engine")))
				storageType = System.Environment.GetEnvironmentVariable("raventest_storage_engine");
			else
				storageType = System.Configuration.ConfigurationManager.AppSettings["Raven/StorageEngine"];

			if(storageType == "munin")
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
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null && (debug  == false || Debugger.IsAttached));
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
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null && Debugger.IsAttached);
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
			BoundedMemoryTarget boundedMemoryTarget = null;
			if (LogManager.Configuration != null && LogManager.Configuration.AllTargets != null)
			{
				boundedMemoryTarget = LogManager.Configuration.AllTargets.OfType<BoundedMemoryTarget>().FirstOrDefault();
			}
			if (boundedMemoryTarget != null)
			{
				boundedMemoryTarget.Clear();
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

					// Delete tenants created using the EnsureDatabaseExists method.
					IOExtensions.DeleteDirectory("Tenants");
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
			var startTime = SystemTime.Now;
			action.Invoke();
			var timeTaken = SystemTime.Now.Subtract(startTime);
			Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
			return timeTaken.TotalMilliseconds;
		}

		public IDocumentStore NewRemoteDocumentStore()
		{
			var ravenDbServer = GetNewServer();
			var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			};

			store.AfterDispose += (sender, args) => ravenDbServer.Dispose();
			ModifyStore(store);
			return store.Initialize();
		}

		public virtual void Dispose()
		{
			ClearDatabaseDirectory();
			GC.Collect(2);
			GC.WaitForPendingFinalizers();
		}
	}
}
