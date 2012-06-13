//-----------------------------------------------------------------------
// <copyright file="RavenTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using NLog;
using Raven.Abstractions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Storage.Managed;
using Raven.Tests.Document;

namespace Raven.Tests
{
	public class RavenTest : WithNLog, IDisposable
	{
		protected const string DbDirectory = @".\TestDb\";
		protected const string DbName = DbDirectory + @"DocDb.esb";

		private string path;

		public EmbeddableDocumentStore NewDocumentStore(string storageType = "munin", bool inMemory = true, int? allocatedMemory = null, bool deleteExisting = true)
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

			var documentStore = new EmbeddableDocumentStore
									{
										Configuration =
											{
												DataDirectory = path,
												RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
												DefaultStorageTypeName = storageType,
												RunInMemory = storageType == "munin" && inMemory,
											}
									};

			ModifyStore(documentStore);
			ModifyConfiguration(documentStore.Configuration);

			if (documentStore.Configuration.RunInMemory == false && deleteExisting)
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

		protected virtual void CreateDefaultIndexes(IDocumentStore documentStore)
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

			documentStore.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.All;
			using (var server = new HttpServer(documentStore.Configuration, documentStore.DocumentDatabase))
			{
				server.StartListening();
				Process.Start(documentStore.Configuration.ServerUrl); // start the server

				do
				{
					Thread.Sleep(100);
				} while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null && Debugger.IsAttached);
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
			IOExtensions.DeleteDirectory(DbName);
			IOExtensions.DeleteDirectory(DbDirectory);

			// Delete tenants created using the EnsureDatabaseExists method.
			IOExtensions.DeleteDirectory("Tenants");
		}

		public double Timer(Action action)
		{
			var startTime = SystemTime.Now;
			action.Invoke();
			var timeTaken = SystemTime.Now.Subtract(startTime);
			Console.WriteLine("Time take (ms)- " + timeTaken.TotalMilliseconds);
			return timeTaken.TotalMilliseconds;
		}

		public virtual void Dispose()
		{
			ClearDatabaseDirectory();
		}
	}
}