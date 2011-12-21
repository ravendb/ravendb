//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Abstractions;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Server;
using Raven.Storage.Managed;
using Raven.Tests.Document;

namespace Raven.Tests
{
	public class RavenTest : WithNLog
	{
		protected const string DbDirectory = @".\TestDb\";
		protected const string DbName = DbDirectory + @"DocDb.esb";

		private string path;

		public EmbeddableDocumentStore NewDocumentStore()
		{
			return NewDocumentStore("munin", true, null);
		}

		public EmbeddableDocumentStore NewDocumentStore(string storageType, bool inMemory)
		{
			return NewDocumentStore(storageType, inMemory, null);
		}

		public EmbeddableDocumentStore NewDocumentStore(string storageType, bool inMemory, int? allocatedMemory)
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

			var documentStore = new EmbeddableDocumentStore()
			{
				Configuration =
					{
						DataDirectory = path,
						RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
						DefaultStorageTypeName = storageType,
						RunInMemory = inMemory,
					}
			};

			ModifyStore(documentStore);
			ModifyConfiguration(documentStore.Configuration);

			if (documentStore.Configuration.RunInMemory == false)
				IOExtensions.DeleteDirectory(path);
			documentStore.Initialize();

			CreateDefaultIndexes(documentStore);

			if (allocatedMemory != null && inMemory)
			{
				var transactionalStorage = ((TransactionalStorage)documentStore.DocumentDatabase.TransactionalStorage);
				transactionalStorage.EnsureCapacity(allocatedMemory.Value);
			}

			return documentStore;
		}

		protected virtual void CreateDefaultIndexes(EmbeddableDocumentStore documentStore)
		{
			new RavenDocumentsByEntityName().Execute(documentStore);
		}

		protected virtual void ModifyStore(EmbeddableDocumentStore documentStore)
		{
			
		}

		static public void WaitForUserToContinueTheTest(EmbeddableDocumentStore documentStore)
		{
			if (Debugger.IsAttached == false)
				return;

			documentStore.DatabaseCommands.Put("Pls Delete Me", null,

			                                   RavenJObject.FromObject(new { StackTrace = new StackTrace(true) }),
			                                   new RavenJObject());

			documentStore.Configuration.AnonymousUserAccessMode=AnonymousUserAccessMode.All;
			using (var server = new HttpServer(documentStore.Configuration, documentStore.DocumentDatabase))
			{
				server.StartListening();
				Process.Start(documentStore.Configuration.ServerUrl); // start the server

				do
				{
					Thread.Sleep(100);
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null);
			}
		}

		protected virtual void ModifyConfiguration(RavenConfiguration configuration)
		{
		}

		public static void WaitForIndexing(EmbeddableDocumentStore store)
		{
			while (store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
			{
				Thread.Sleep(100);
			}
		}

		protected void WaitForAllRequestsToComplete(RavenDbServer server)
		{
			while (server.Server.HasPendingRequests)
				Thread.Sleep(25);
		}

		protected RavenDbServer GetNewServer(bool initializeDocumentsByEntitiyName = true)
		{
			var ravenConfiguration = new RavenConfiguration
			{
				Port = 8079,
				RunInMemory = true,
				DataDirectory = "Data",
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			};

			ConfigureServer(ravenConfiguration);

			if(ravenConfiguration.RunInMemory == false)
				IOExtensions.DeleteDirectory(ravenConfiguration.DataDirectory);

			var ravenDbServer = new RavenDbServer(ravenConfiguration);

			if (initializeDocumentsByEntitiyName)
			{
				try
				{
					using (var documentStore = new DocumentStore
					{
						Url = "http://localhost:8079"
					}.Initialize())
					{
						new RavenDocumentsByEntityName().Execute(documentStore);
					}
				}
				catch
				{
					ravenDbServer.Dispose();
					throw;
				}
			}

			return ravenDbServer;
		}

		protected virtual void ConfigureServer(RavenConfiguration ravenConfiguration)
		{
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

		protected RavenDbServer GetNewServer(int port, string path)
		{
			var ravenDbServer = new RavenDbServer(new RavenConfiguration
			{
				Port = port,
				DataDirectory = path,
				RunInMemory = true,
				AnonymousUserAccessMode = AnonymousUserAccessMode.All
			});

			try
			{
				using (var documentStore = new DocumentStore
				{
					Url = "http://localhost:" + port
				}.Initialize())
				{
					new RavenDocumentsByEntityName().Execute(documentStore);
				}
			}
			catch 
			{
				ravenDbServer.Dispose();
				throw;
			}
			return ravenDbServer;
		}

		protected RavenDbServer GetNewServerWithoutAnonymousAccess(int port, string path)
		{
			var newServerWithoutAnonymousAccess = new RavenDbServer(new RavenConfiguration { Port = port, DataDirectory = path, AnonymousUserAccessMode = AnonymousUserAccessMode.None });
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:" + port
			}.Initialize())
			{
				new RavenDocumentsByEntityName().Execute(documentStore);
			}
			return newServerWithoutAnonymousAccess;
		}

		protected string GetPath(string subFolderName)
		{
			string retPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			return Path.Combine(retPath, subFolderName).Substring(6); //remove leading file://
		}

		public RavenTest()
		{
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
			IOExtensions.DeleteDirectory(DbName);
			IOExtensions.DeleteDirectory(DbDirectory);
		}

		public double Timer(Action action)
		{
			var startTime = SystemTime.Now;
			action.Invoke();
			var timeTaken = SystemTime.Now.Subtract(startTime);
			Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
			return timeTaken.TotalMilliseconds;
		}
	}
}