//-----------------------------------------------------------------------
// <copyright file="ReplicationBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
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
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.Common
{
    public abstract class ReplicationBase : RavenTest
    {
        protected int PortRangeStart = 8079;
        protected int RetriesCount = 500;

        public ReplicationBase()
        {
            checkPorts = true;
        }

        public DocumentStore CreateStore(bool enableCompressionBundle = false, Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, bool enableAuthorization = false, string requestedStorageType = "esent", bool useFiddler = false, [CallerMemberName] string databaseName = null)
        {
            var port = PortRangeStart - stores.Count;
            return CreateStoreAtPort(port, enableCompressionBundle, configureStore, anonymousUserAccessMode, enableAuthorization, requestedStorageType, useFiddler, databaseName);
        }

        public EmbeddableDocumentStore CreateEmbeddableStore(bool enableCompressionBundle = false,
			AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, string requestedStorageType = "esent", [CallerMemberName] string databaseName = null)
        {
            var port = PortRangeStart - stores.Count;
            return CreateEmbeddableStoreAtPort(port, enableCompressionBundle, anonymousUserAccessMode, requestedStorageType, databaseName);
        }

        private DocumentStore CreateStoreAtPort(int port, bool enableCompressionBundle = false,
            Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, bool enableAuthorization = false, string storeTypeName = "esent", bool useFiddler = false, string databaseName = null)
        {
            var ravenDbServer = GetNewServer(port,
                requestedStorage: storeTypeName,
                activeBundles: "replication" + (enableCompressionBundle ? ";compression" : string.Empty),
                enableAuthentication: anonymousUserAccessMode == AnonymousUserAccessMode.None,
                databaseName: databaseName, 
				configureConfig: ConfigureConfig, 
                configureServer: ConfigureServer);

            if (enableAuthorization)
            {
				EnableAuthentication(ravenDbServer.SystemDatabase);
            }

			var documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer, configureStore: configureStore, fiddler: useFiddler, databaseName: databaseName);

			ConfigureDatabase(ravenDbServer.SystemDatabase, databaseName: databaseName);

            return documentStore;
        }

        protected virtual void ConfigureServer(RavenDBOptions options)
        {
            
        }

        protected virtual void ConfigureConfig(InMemoryRavenConfiguration inMemoryRavenConfiguration)
	    {
		    
	    }

	    private EmbeddableDocumentStore CreateEmbeddableStoreAtPort(int port, bool enableCompressionBundle = false, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.All, string storeTypeName = "esent", string databaseName = null)
        {
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);
			var store = NewDocumentStore(port: port,
				requestedStorage:storeTypeName,
				activeBundles: "replication" + (enableCompressionBundle ? ";compression" : string.Empty),
				anonymousUserAccessMode: anonymousUserAccessMode,
                databaseName: databaseName);
			return store;
		}

        protected virtual void ConfigureDatabase(DocumentDatabase database, string databaseName = null)
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
			ModifyConfiguration(serverConfiguration);

            serverConfiguration.PostInit();
            var ravenDbServer = new RavenDbServer(serverConfiguration)
            {
                UseEmbeddedHttpServer = true
            };
            ravenDbServer.Initialize(ConfigureServer);

            servers[index] = ravenDbServer;
        }

        public IDocumentStore ResetDatabase(int index, bool enableAuthentication = false, [CallerMemberName] string databaseName = null)
        {
            stores[index].Dispose();

            var previousServer = servers[index];
            previousServer.Dispose();
			IOExtensions.DeleteDirectory(previousServer.SystemDatabase.Configuration.DataDirectory);

			return CreateStoreAtPort(previousServer.SystemDatabase.Configuration.Port, enableAuthentication, databaseName: databaseName);
        }

		protected void TellFirstInstanceToReplicateToSecondInstance(string apiKey = null, string username = null, string password = null, string domain = null)
        {
			TellInstanceToReplicateToAnotherInstance(0, 1, apiKey, username, password, domain);
        }

		protected void TellSecondInstanceToReplicateToFirstInstance(string apiKey = null, string username = null, string password = null, string domain = null)
        {
			TellInstanceToReplicateToAnotherInstance(1, 0, apiKey, username, password, domain);
        }

		protected void TellInstanceToReplicateToAnotherInstance(int src, int dest, string apiKey = null, string username = null, string password = null, string domain = null)
        {
			RunReplication(stores[src], stores[dest], apiKey: apiKey, username: username, password: password, domain: domain);
        }

        protected void RunReplication(IDocumentStore source, IDocumentStore destination,
            TransitiveReplicationOptions transitiveReplicationBehavior = TransitiveReplicationOptions.None,
            bool disabled = false,
            bool ignoredClient = false,
            string apiKey = null,
			string db = null,
			string username = null,
			string password = null,
			string domain = null)
        {
            db = db ?? (destination is DocumentStore ? ((DocumentStore)destination).DefaultDatabase : null);

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
				if (username != null)
				{
					replicationDestination.Username = username;
					replicationDestination.Password = password;
					replicationDestination.Domain = domain;
				}

                SetupDestination(replicationDestination);
	            Console.WriteLine("writing rep dests for " + db + " " + source.Url);
                session.Store(new ReplicationDocument
                {
                    Destinations = { replicationDestination }
                }, "Raven/Replication/Destinations");
	            session.SaveChanges();
            }

	        while (true)
	        {
		        using (var s = source.OpenSession(db))
		        {
			        var doc = s.Load<ReplicationDocument>("Raven/Replication/Destinations");
			        if (string.IsNullOrWhiteSpace(doc.Source))
			        {
				        Thread.Sleep(100);
						continue;
			        }
			        break;
		        }
	        }
        }

        protected virtual void SetupDestination(ReplicationDestination replicationDestination)
        {

        }

        protected void SetupReplication(IDatabaseCommands source, params DocumentStore[] destinations)
        {
            Assert.NotEmpty(destinations);
            SetupReplication(source, destinations.Select(destination => new RavenJObject
                                                                        {
                                                                            { "Url", destination.Url },
                                                                            { "Database", destination.DefaultDatabase }
                                                                        }));
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


			using (var session = store.OpenSession(db))
			{
				var e = session.Load<object>(id);
				Assert.NotNull(e);
			}
        }

        protected void WaitForReplication(IDocumentStore store, Func<IDocumentSession, bool> predicate, string db = null)
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
						{"Name", string.Join(" ", conflictedDocs.Select(x => x.DataAsJson.Value<string>("Name")).OrderBy(x => x))}
					},
                    Metadata = new RavenJObject()
                };
                return true;
            }
        }

        protected async Task PauseReplicationAsync(int serverIndex, string databaseName, bool waitToStop = true)
        {
            var database = await servers[serverIndex].Server.GetDatabaseInternal(databaseName);
            var replicationTask = database.StartupTasks.OfType<ReplicationTask>().First();

            replicationTask.Pause();

            if (waitToStop)
                SpinWait.SpinUntil(() => replicationTask.IsRunning == false, TimeSpan.FromSeconds(10));
        }

        protected async Task ContinueReplicationAsync(int serverIndex, string databaseName, bool waitToStart = true)
        {
            var database = await servers[serverIndex].Server.GetDatabaseInternal(databaseName);
            var replicationTask = database.StartupTasks.OfType<ReplicationTask>().First();

            replicationTask.Continue();

            if (waitToStart)
                SpinWait.SpinUntil(() => replicationTask.IsRunning, TimeSpan.FromSeconds(10));
        }
    }
}
