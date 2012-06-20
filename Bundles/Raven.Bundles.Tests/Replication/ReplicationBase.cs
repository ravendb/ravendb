//-----------------------------------------------------------------------
// <copyright file="ReplicationBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias replication;
extern alias database;

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Replication;
using Raven.Bundles.Tests.Versioning;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;
using IOExtensions = database::Raven.Database.Extensions.IOExtensions;
using System.Linq;

namespace Raven.Bundles.Tests.Replication
{
	public class ReplicationBase : IDisposable
	{
		private readonly List<IDocumentStore> stores = new List<IDocumentStore>();
		protected readonly List<RavenDbServer> servers = new List<RavenDbServer>();

		private const int PortRangeStart = 8079;
		protected const int RetriesCount = 500;

		public IDocumentStore CreateStore(Action<DocumentStore> configureStore = null)
		{
			var port = PortRangeStart - servers.Count;
			return CreateStoreAtPort(port, configureStore);
		}

		private IDocumentStore CreateStoreAtPort(int port, Action<DocumentStore> configureStore = null)
		{
			database::Raven.Database.Server.NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
			var assemblyCatalog = new AssemblyCatalog(typeof (replication::Raven.Bundles.Replication.Triggers.AncestryPutTrigger).Assembly);
			var serverConfiguration = new database::Raven.Database.Config.RavenConfiguration
			                          {
			                          	AnonymousUserAccessMode = database::Raven.Database.Server.AnonymousUserAccessMode.All,
			                          	Catalog = {Catalogs = {assemblyCatalog}},
			                          	DataDirectory = "Data #" + servers.Count,
			                          	RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			                          	RunInMemory = true,
			                          	Port = port
			                          };
			ConfigureServer(serverConfiguration);
			IOExtensions.DeleteDirectory(serverConfiguration.DataDirectory);
			serverConfiguration.PostInit();
			var ravenDbServer = new RavenDbServer(serverConfiguration);
			ravenDbServer.Server.SetupTenantDatabaseConfiguration += configuration => configuration.Catalog.Catalogs.Add(assemblyCatalog);
			servers.Add(ravenDbServer);
			
			var documentStore = new DocumentStore {Url = ravenDbServer.Database.Configuration.ServerUrl};
			ConfigureStore(documentStore);
			if (configureStore != null)
				configureStore(documentStore);
			documentStore.Initialize();
			documentStore.JsonRequestFactory.EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = true;

			stores.Add(documentStore);
			return documentStore;
		}

		protected virtual void ConfigureServer(database::Raven.Database.Config.RavenConfiguration serverConfiguration)
		{
		}

		protected virtual void ConfigureStore(DocumentStore documentStore)
		{
		}

		public void Dispose()
		{
			var err = new List<Exception>();
			foreach (var documentStore in stores)
			{
				try
				{
					documentStore.Dispose();
				}
				catch (Exception e)
				{
					err.Add(e);	
				}
			}

			foreach (var ravenDbServer in servers)
			{
				try
				{
					ravenDbServer.Dispose();
					IOExtensions.DeleteDirectory(ravenDbServer.Database.Configuration.DataDirectory);
				}
				catch (Exception e)
				{
					err.Add(e);
				}
			}

			if (err.Count > 0)
				throw new AggregateException(err);
		}

		public IDocumentStore ResetDatabase(int index)
		{
			stores[index].Dispose();

			var previousServer = servers[index];
			previousServer.Dispose();
			IOExtensions.DeleteDirectory(previousServer.Database.Configuration.DataDirectory);

			return CreateStoreAtPort(previousServer.Database.Configuration.Port);
		}

		protected void TellFirstInstanceToReplicateToSecondInstance()
		{
			TellInstanceToReplicateToAnotherInstance(0, 1);
		}

		protected void TellSecondInstanceToReplicateToFirstInstance()
		{
			TellInstanceToReplicateToAnotherInstance(1, 0);
		}

		protected void TellInstanceToReplicateToAnotherInstance(int src, int dest)
		{
			RunReplication(stores[src], stores[dest]);
		}

		protected void RunReplication(IDocumentStore source, IDocumentStore destination, TransitiveReplicationOptions transitiveReplicationBehavior = TransitiveReplicationOptions.None)
		{
			Console.WriteLine("Replicating from {0} to {1}.", source.Url, destination.Url);
			using (var session = source.OpenSession())
			{
				var replicationDestination = new ReplicationDestination
				{
					Url = destination.Url.Replace("localhost", "ipv4.fiddler"),
					TransitiveReplicationBehavior = transitiveReplicationBehavior,
				};
				SetupDestination(replicationDestination);
				session.Store(new ReplicationDocument
				{
					Destinations = {replicationDestination}
				}, "Raven/Replication/Destinations");
				session.SaveChanges();
			}
		}

		protected virtual void SetupDestination(ReplicationDestination replicationDestination)
		{
			
		}

		protected void SetupReplication(IDatabaseCommands source, params string[] urls)
		{
			Assert.NotEmpty(urls);
			source.Put(replication::Raven.Bundles.Replication.ReplicationConstants.RavenReplicationDestinations,
			           null, new RavenJObject
			           {
			           	{
			           		"Destinations", new RavenJArray(urls.Select(url => new RavenJObject
			           		{
			           			{"Url", url}
			           		}))
			           		}
			           }, new RavenJObject());
		}

		protected TDocument WaitForDocument<TDocument>(IDocumentStore store2, string expectedId) where TDocument : class
		{
			TDocument document = null;

			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenSession())
				{
					document = session.Load<TDocument>(expectedId);
					if (document != null)
						break;
					Thread.Sleep(100);
				}
			}
			try
			{
				Assert.NotNull(document);
			}
			catch (Exception ex)
			{
				using (var session = store2.OpenSession())
				{
					Thread.Sleep(TimeSpan.FromSeconds(10));

					document = session.Load<TDocument>(expectedId);
					if (document == null)
						throw;

					throw new Exception("WaitForDocument failed, but after waiting 10 seconds more, WaitForDocument succeed. Do we have a race condition here?", ex);
				}
			}
			return document;
		}

		protected void WaitForDocument(IDatabaseCommands commands, string expectedId)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				if (commands.Head(expectedId) != null)
					break;
				Thread.Sleep(100);
			}

			var jsonDocumentMetadata = commands.Head(expectedId);
			
			Assert.NotNull(jsonDocumentMetadata);
		}

		protected void WaitForReplication(IDocumentStore store2, string id)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store2.OpenSession())
				{
					var company = session.Load<object>(id);
					if (company != null)
						break;
					Thread.Sleep(100);
				}
			}
		}
	}
}