//-----------------------------------------------------------------------
// <copyright file="ReplicationBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Listeners;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bundles.Replication
{
	public class ReplicationBase : RavenTest
	{
		protected int PortRangeStart = 8079;
		protected int RetriesCount = 500;

		public IDocumentStore CreateStore(bool enableCompressionBundle = false, bool removeDataDirectory = true, Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, bool enableAuthorization = false)
		{
			var port = PortRangeStart - stores.Count;
			return CreateStoreAtPort(port, enableCompressionBundle, removeDataDirectory, configureStore, anonymousUserAccessMode, enableAuthorization);
		}

        public EmbeddableDocumentStore CreateEmbeddableStore(bool enableCompressionBundle = false, bool removeDataDirectory = true, Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin)
		{
			var port = PortRangeStart - stores.Count;
			return CreateEmbeddableStoreAtPort(port, enableCompressionBundle, removeDataDirectory, configureStore, anonymousUserAccessMode);
		}

        private IDocumentStore CreateStoreAtPort(int port, bool enableCompressionBundle = false, bool removeDataDirectory = true, Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, bool enableAuthorization = false)
		{
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
			var serverConfiguration = new RavenConfiguration
									  {
										  Settings = { { "Raven/ActiveBundles", "replication" + (enableCompressionBundle ? ";compression" : string.Empty) } },
										  AnonymousUserAccessMode = anonymousUserAccessMode,
										  DataDirectory = "Data #" + stores.Count,
										  RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
										  RunInMemory = true,
										  Port = port,
										  UseFips = SettingsHelper.UseFipsEncryptionAlgorithms,
										  DefaultStorageTypeName = RavenTest.GetDefaultStorageType()
									  };

			ConfigureServer(serverConfiguration);
			if (removeDataDirectory)
			{
				IOExtensions.DeleteDirectory(serverConfiguration.DataDirectory);
			}

			serverConfiguration.PostInit();
			var ravenDbServer = new RavenDbServer(serverConfiguration);
			servers.Add(ravenDbServer);

			if (enableAuthorization)
			{
				EnableAuthentication(ravenDbServer.SystemDatabase);
				ConfigureServer(serverConfiguration);
			}

			var documentStore = new DocumentStore { Url = ravenDbServer.SystemDatabase.Configuration.ServerUrl };
			ConfigureStore(documentStore);
			if (configureStore != null)
				configureStore(documentStore);
			documentStore.Initialize();

			stores.Add(documentStore);

			ConfigureDatabase(ravenDbServer.SystemDatabase);
			return documentStore;
		}

		private EmbeddableDocumentStore CreateEmbeddableStoreAtPort(int port, bool enableCompressionBundle = false, bool removeDataDirectory = true, Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.All)
		{
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

			var embeddedStore = new EmbeddableDocumentStore
			{
				UseEmbeddedHttpServer = true,
				Configuration =
				{
					Settings = { { "Raven/ActiveBundles", "replication" + (enableCompressionBundle ? ";compression" : string.Empty) } },
					AnonymousUserAccessMode = anonymousUserAccessMode,
					DataDirectory = "Data #" + stores.Count,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					RunInMemory = true,
					Port = port,
					UseFips = SettingsHelper.UseFipsEncryptionAlgorithms,
					DefaultStorageTypeName = GetDefaultStorageType()
				},
			};

			if (removeDataDirectory)
			{
				IOExtensions.DeleteDirectory(embeddedStore.Configuration.DataDirectory);
			}

			ConfigureStore(embeddedStore);
			if (configureStore != null)
				configureStore(embeddedStore);
			embeddedStore.Initialize();

			stores.Add(embeddedStore);

			return embeddedStore;
		}

		protected virtual void ConfigureServer(RavenConfiguration serverConfiguration)
		{
		}

		protected virtual void ConfigureDatabase(DocumentDatabase database)
		{
			
		}

		protected virtual void ConfigureStore(DocumentStore documentStore)
		{
		}

		public void StopDatabase(int index)
		{
			var previousServer = servers[index];
			previousServer.Dispose();
		}

		public void StartDatabase(int index)
		{
			var previousServer = servers[index];

			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(previousServer.SystemDatabase.Configuration.Port);
			var serverConfiguration = new RavenConfiguration
			{
				Settings = { { "Raven/ActiveBundles", "replication" } },
                AnonymousUserAccessMode = AnonymousUserAccessMode.Admin,
				DataDirectory = previousServer.SystemDatabase.Configuration.DataDirectory,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				RunInMemory = previousServer.SystemDatabase.Configuration.RunInMemory,
				Port = previousServer.SystemDatabase.Configuration.Port,
				UseFips = SettingsHelper.UseFipsEncryptionAlgorithms,
				DefaultStorageTypeName = GetDefaultStorageType()
			};
			ConfigureServer(serverConfiguration);

			serverConfiguration.PostInit();
			var ravenDbServer = new RavenDbServer(serverConfiguration);

			servers[index] = ravenDbServer;
		}

		public IDocumentStore ResetDatabase(int index, bool enableAuthentication = false)
		{
			stores[index].Dispose();

			var previousServer = servers[index];
			previousServer.Dispose();
			IOExtensions.DeleteDirectory(previousServer.SystemDatabase.Configuration.DataDirectory);

			return CreateStoreAtPort(previousServer.SystemDatabase.Configuration.Port, enableAuthentication);
		}

		protected void TellFirstInstanceToReplicateToSecondInstance(string apiKey = null)
		{
			TellInstanceToReplicateToAnotherInstance(0, 1, apiKey);
		}

		protected void TellSecondInstanceToReplicateToFirstInstance(string apiKey = null)
		{
			TellInstanceToReplicateToAnotherInstance(1, 0, apiKey);
		}

		protected void TellInstanceToReplicateToAnotherInstance(int src, int dest, string apiKey = null)
		{
			RunReplication(stores[src], stores[dest], apiKey: apiKey);
		}

		protected void RunReplication(IDocumentStore source, IDocumentStore destination,
			TransitiveReplicationOptions transitiveReplicationBehavior = TransitiveReplicationOptions.None,
			bool disabled = false,
			bool ignoredClient = false,
			string apiKey = null,
			string db = null)
		{
			Console.WriteLine("Replicating from {0} to {1} with db = {2}.", source.Url, destination.Url, db ?? Constants.SystemDatabase);
			using (var session = source.OpenSession(db))
			{
				var replicationDestination = new ReplicationDestination
				{
					Url = destination is EmbeddableDocumentStore ? 
							"http://localhost:" + (destination as EmbeddableDocumentStore).Configuration.Port :
							destination.Url.Replace("localhost", "ipv4.fiddler"),
					TransitiveReplicationBehavior = transitiveReplicationBehavior,
					Disabled = disabled,
					IgnoredClient = ignoredClient
				};
				if (db != null)
					replicationDestination.Database = db;
				if (apiKey != null)
					replicationDestination.ApiKey = apiKey;
				SetupDestination(replicationDestination);
				session.Store(new ReplicationDocument
				{
					Destinations = { replicationDestination }
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
            SetupReplication(source, urls.Select(url => new RavenJObject { { "Url", url } }));
		}

        protected void SetupReplication(IDatabaseCommands source, IEnumerable<RavenJObject> destinations)
        {
            Assert.NotEmpty(destinations);
            source.Put(Constants.RavenReplicationDestinations,
                       null, new RavenJObject
                       {
                           {
                               "Destinations", new RavenJArray(destinations)
                           }
                       }, new RavenJObject());
        }

		protected void RemoveReplication(IDatabaseCommands source)
		{
			source.Put(
				Constants.RavenReplicationDestinations,
				null,
				new RavenJObject 
				{
					                 {
						                 "Destinations", new RavenJArray()
					                 } 
				},
				new RavenJObject());
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

		protected Attachment WaitForAttachment(IDocumentStore store2, string expectedId)
		{
			Attachment attachment = null;

			for (int i = 0; i < RetriesCount; i++)
			{
				attachment = store2.DatabaseCommands.GetAttachment(expectedId);
				if (attachment != null)
					break;
				Thread.Sleep(100);
			}
			try
			{
				Assert.NotNull(attachment);
			}
			catch (Exception ex)
			{
				Thread.Sleep(TimeSpan.FromSeconds(10));

				attachment = store2.DatabaseCommands.GetAttachment(expectedId);
				if (attachment == null) throw;

				throw new Exception(
					"WaitForDocument failed, but after waiting 10 seconds more, WaitForDocument succeed. Do we have a race condition here?",
					ex);
			}
			return attachment;
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

		protected void WaitForReplication(IDocumentStore store, string id, string db = null, Etag changedSince = null)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store.OpenSession(db))
				{
					var e = session.Load<object>(id);
					if (e == null)
					{
						if (changedSince != null)
						{
							if (session.Advanced.GetEtagFor(e) != changedSince)
								break;
						}
						Thread.Sleep(100);
						continue;
					}
					
					break;
				}
			}
		}

		protected void WaitForReplication(IDocumentStore store, Func<IDocumentSession, bool>  predicate, string db = null)
		{
			for (int i = 0; i < RetriesCount; i++)
			{
				using (var session = store.OpenSession(new OpenSessionOptions
				{
					Database = db,
					ForceReadFromMaster = true
				}))
				{
					if (predicate(session))
						return;
					Thread.Sleep(100);
				}
			}
		}

		protected class ClientSideConflictResolution : IDocumentConflictListener
		{
			public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
			{
				resolvedDocument = new JsonDocument
				{
					DataAsJson = new RavenJObject
					 {
						 {"Name", string.Join(" ", conflictedDocs.Select(x => x.DataAsJson.Value<string>("Name")).OrderBy(x=>x))}
					 },
					Metadata = new RavenJObject()
				};
				return true;
			}
		}
	}
}
