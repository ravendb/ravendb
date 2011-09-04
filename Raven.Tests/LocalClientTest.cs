//-----------------------------------------------------------------------
// <copyright file="LocalClientTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Xml;
using NLog.Config;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Storage.Managed;
using Raven.Tests.Document;

namespace Raven.Tests
{
	public abstract class LocalClientTest : WithNLog
	{
		private string path;
		
		protected void EnableDebugLog()
		{
			var nlogPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NLog.config");
			if (File.Exists(nlogPath))
				return;// that overrides the default config

			using (var stream = typeof(LocalClientTest).Assembly.GetManifestResourceStream("Raven.Tests.DefaultLogging.config"))
			using (var reader = XmlReader.Create(stream))
			{
				NLog.LogManager.Configuration = new XmlLoggingConfiguration(reader, "default-config");
			}
		}

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

		static public void WaitForUserToContinueTheTest(EmbeddableDocumentStore documentStore)
		{
			if (Debugger.IsAttached == false)
				return;

			documentStore.DatabaseCommands.Put("Pls Delete Me", null,

											   RavenJObject.FromObject(new { StackTrace = new StackTrace(true) }),
											   new RavenJObject());

			using (var server = new RavenDbHttpServer(documentStore.Configuration, documentStore.DocumentDatabase))
			{
				server.Start();
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

		public void WaitForIndexing(EmbeddableDocumentStore store)
		{
			while (store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
			{
				Thread.Sleep(100);
			}
		}
	}
}
