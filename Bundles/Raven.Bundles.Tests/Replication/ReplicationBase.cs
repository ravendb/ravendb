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
using System.Threading;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;
using IOExtensions = database::Raven.Database.Extensions.IOExtensions;

namespace Raven.Bundles.Tests.Replication
{
	public class ReplicationBase : IDisposable
	{
		private readonly List<IDocumentStore> stores = new List<IDocumentStore>();
		protected readonly List<RavenDbServer> servers = new List<RavenDbServer>();

		public ReplicationBase()
		{
			for (int i = 0; i < 15; i++)
			{
				database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Data #" + i);
			}
		}

		private const int PortRangeStart = 8500;
		protected const int RetriesCount = 300;

		public IDocumentStore CreateStore()
		{
			var port = PortRangeStart + servers.Count;
			return CreateStoreAtPort(port);
		}

		private IDocumentStore CreateStoreAtPort(int port)
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
			serverConfiguration.PostInit();
			var ravenDbServer = new RavenDbServer(serverConfiguration);
			ravenDbServer.Server.SetupTenantDatabaseConfiguration += configuration => configuration.Catalog.Catalogs.Add(assemblyCatalog);
			servers.Add(ravenDbServer);
			var documentStore = new DocumentStore {Url = ravenDbServer.Database.Configuration.ServerUrl};
			ConfigureStore(documentStore);
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
			foreach (var documentStore in stores)
			{
				documentStore.Dispose();
			}

			foreach (var ravenDbServer in servers)
			{
				ravenDbServer.Dispose();
				IOExtensions.DeleteDirectory(ravenDbServer.Database.Configuration.DataDirectory);
			}
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
					Url = destination.Url,
					TransitiveReplicationBehavior = transitiveReplicationBehavior,
				};
				SetupDestination(replicationDestination);
				session.Store(new ReplicationDocument
				{
					Destinations = {replicationDestination}
				});
				session.SaveChanges();
			}
		}

		protected virtual void SetupDestination(ReplicationDestination replicationDestination)
		{
			
		}

		protected void SetupReplication(IDatabaseCommands source, string url)
		{
			source.Put(replication::Raven.Bundles.Replication.ReplicationConstants.RavenReplicationDestinations,
			           null, new RavenJObject
			                 {
			                 	{
			                 	"Destinations", new RavenJArray
			                 	                {
			                 	                	new RavenJObject
			                 	                	{
			                 	                		{"Url", url}
			                 	                	}
			                 	                }
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
			Assert.NotNull(document);
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
			Assert.NotNull(commands.Head(expectedId));
		}
	}
}