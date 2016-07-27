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
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
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
using Raven.Tests.Helpers.Util;

using Xunit;

namespace Raven.Tests.Common
{
    public abstract class ReplicationBase : RavenTest
    {
        protected int PortRangeStart = 9000;
        protected int RetriesCount = 500;
        private volatile bool hasWaitEnded;

        protected ReplicationBase()
        {
            checkPorts = true;
        }

        public DocumentStore CreateStore(bool enableCompressionBundle = false,
            Action<DocumentStore> configureStore = null,
            AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin,
            bool enableAuthorization = false,
            string requestedStorageType = "voron",
            bool useFiddler = false,
            [CallerMemberName] string databaseName = null,
            bool runInMemory = true)
        {
            var port = PortRangeStart - stores.Count;
            return CreateStoreAtPort(port, enableCompressionBundle, configureStore, anonymousUserAccessMode, enableAuthorization, requestedStorageType, useFiddler, databaseName, runInMemory);
        }

        public EmbeddableDocumentStore CreateEmbeddableStore(bool enableCompressionBundle = false,
            AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, string requestedStorageType = "esent", [CallerMemberName] string databaseName = null)
        {
            var port = PortRangeStart - stores.Count;
            return CreateEmbeddableStoreAtPort(port, enableCompressionBundle, anonymousUserAccessMode, requestedStorageType, databaseName);
        }

        private DocumentStore CreateStoreAtPort(int port, bool enableCompressionBundle = false,
            Action<DocumentStore> configureStore = null, AnonymousUserAccessMode anonymousUserAccessMode = AnonymousUserAccessMode.Admin, bool enableAuthorization = false,
            string storeTypeName = "voron", bool useFiddler = false, string databaseName = null, bool runInMemory = true)
        {
            var ravenDbServer = GetNewServer(port,
                requestedStorage: storeTypeName,
                activeBundles: "replication" + (enableCompressionBundle ? ";compression" : string.Empty),
                enableAuthentication: anonymousUserAccessMode == AnonymousUserAccessMode.None,
                databaseName: databaseName,
                configureConfig: ConfigureConfig,
                configureServer: ConfigureServer,
                runInMemory: runInMemory);

            if (enableAuthorization)
            {
                EnableAuthentication(ravenDbServer.SystemDatabase);
            }

            ConfigureDatabase(ravenDbServer.SystemDatabase, databaseName: databaseName);

            var documentStore = NewRemoteDocumentStore(ravenDbServer: ravenDbServer,
                configureStore: configureStore,
                fiddler: useFiddler,
                databaseName: databaseName,
                runInMemory: runInMemory);

            return documentStore;
        }


        protected bool CheckIfConflictDocumentsIsThere(IDocumentStore store, string id, string databaseName, int maxDocumentsToCheck = 1024, int timeoutMs = 15000)
        {
            var beginningTime = DateTime.UtcNow;
            var timeouted = false;
            JsonDocument[] docs;
            do
            {
                var currentTime = DateTime.UtcNow;
                if ((currentTime - beginningTime).TotalMilliseconds >= timeoutMs)
                {
                    timeouted = true;
                    break;
                }
                docs = store.DatabaseCommands.ForDatabase(databaseName).GetDocuments(0, maxDocumentsToCheck);
            } while (docs.Any(d => d.Key.Contains(id + "/conflicts")));

            return !timeouted;
        }

        protected bool WaitForConflictDocumentsCore(Func<JsonDocument[],bool> conditionFunc ,IDocumentStore store, string id, string databaseName, int maxDocumentsToCheck = 1024, int timeoutMs = 15000)
        {
            var beginningTime = DateTime.UtcNow;
            var timeouted = false;
            JsonDocument[] docs;
            do
            {
                var currentTime = DateTime.UtcNow;
                if ((currentTime - beginningTime).TotalMilliseconds >= timeoutMs)
                {
                    timeouted = true;
                    break;
                }
                docs = store.DatabaseCommands.ForDatabase(databaseName).GetDocuments(0, maxDocumentsToCheck);
            } while (conditionFunc(docs));

            return !timeouted;
        }
        protected bool WaitForConflictDocumentsToAppear(IDocumentStore store, string id, string databaseName, int maxDocumentsToCheck = 1024, int timeoutMs = 15000)
        {
            return WaitForConflictDocumentsCore(docs => !docs.Any(d => d.Key.Contains(id + "/conflicts")), store, id, databaseName, maxDocumentsToCheck, timeoutMs);
        }

        protected bool WaitForConflictDocumentsToDisappear(IDocumentStore store, string id, string databaseName, int maxDocumentsToCheck = 1024, int timeoutMs = 15000)
        {
            return WaitForConflictDocumentsCore(docs =>docs.Any(d => d.Key.Contains(id + "/conflicts")), store, id, databaseName, maxDocumentsToCheck, timeoutMs);
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
                requestedStorage: storeTypeName,
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
            var serverConfiguration = new RavenConfiguration { Settings = { { "Raven/ActiveBundles", "replication" } } };

            ConfigurationHelper.ApplySettingsToConfiguration(serverConfiguration);

            serverConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.Admin;
            serverConfiguration.DataDirectory = previousServer.SystemDatabase.Configuration.DataDirectory;
            serverConfiguration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true;
            serverConfiguration.RunInMemory = previousServer.SystemDatabase.Configuration.RunInMemory;
            serverConfiguration.Port = previousServer.SystemDatabase.Configuration.Port;
            serverConfiguration.DefaultStorageTypeName = GetDefaultStorageType();

            serverConfiguration.Encryption.UseFips = ConfigurationHelper.UseFipsEncryptionAlgorithms;

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

        protected void TellFirstInstanceToReplicateToSecondInstance(string apiKey = null, string username = null, string password = null, string domain = null, string authenticationScheme = null)
        {
            TellInstanceToReplicateToAnotherInstance(0, 1, apiKey, username, password, domain, authenticationScheme);
        }

        protected void TellSecondInstanceToReplicateToFirstInstance(string apiKey = null, string username = null, string password = null, string domain = null, string authenticationScheme = null)
        {
            TellInstanceToReplicateToAnotherInstance(1, 0, apiKey, username, password, domain);
        }

        protected void TellInstanceToReplicateToAnotherInstance(int src, int dest, string apiKey = null, string username = null, string password = null, string domain = null, string authenticationScheme = null)
        {
            RunReplication(stores[src], stores[dest], apiKey: apiKey, username: username, password: password, domain: domain, authenticationScheme: authenticationScheme);
        }

        protected void RunReplication(IDocumentStore source, IDocumentStore destination,
            TransitiveReplicationOptions transitiveReplicationBehavior = TransitiveReplicationOptions.None,
            bool disabled = false,
            bool ignoredClient = false,
            string apiKey = null,
            string db = null,
            string username = null,
            string password = null,
            string domain = null,
            ReplicationClientConfiguration clientConfiguration = null,
            Dictionary<string, string> specifiedCollections = null,
            bool skipIndexReplication = false,
            string authenticationScheme = null)
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
                    IgnoredClient = ignoredClient,
                    AuthenticationScheme = authenticationScheme,
                    SpecifiedCollections = specifiedCollections,
                    SkipIndexReplication = skipIndexReplication
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
                    Destinations = { replicationDestination },
                    ClientConfiguration = clientConfiguration

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


        protected void SetupReplication(IDatabaseCommands source, Dictionary<string, string> specifiedCollections, params DocumentStore[] destinations)
        {
            Assert.NotEmpty(destinations);

            var destinationDocs = destinations.Select(destination => new RavenJObject
                {
                    { "Url", destination.Url },
                    { "Database", destination.DefaultDatabase },
                    { "SpecifiedCollections", RavenJObject.FromObject(specifiedCollections) }
                }).ToList();			

            SetupReplication(source, destinationDocs);
        }


        protected void SetupReplication(IDatabaseCommands source, params DocumentStore[] destinations)
        {
            Assert.NotEmpty(destinations);


            var destinationDocs = destinations.Select(destination => new RavenJObject
                                                                        {
                                                                            { "Url", destination.Url },
                                                                            { "Database", destination.DefaultDatabase }
                }).ToList();

            SetupReplication(source, destinationDocs);
        }

        protected void UpdateReplication(IDatabaseCommands source, params DocumentStore[] destinations)
                                                                        {
            Assert.NotEmpty(destinations);


            var destinationDocs = destinations.Select(destination => new RavenJObject
                {
                                                                            { "Url", destination.Url },
                                                                            { "Database", destination.DefaultDatabase }
                }).ToList();

            UpdateReplication(source, destinationDocs);
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

        protected void UpdateReplication(IDatabaseCommands source, IEnumerable<RavenJObject> destinations)
        {
            Assert.NotEmpty(destinations);
            var patches = new List<PatchRequest>();
            foreach (var dest in destinations)
            {
                patches.Add(new PatchRequest
                {
                    Type = PatchCommandType.Insert,
                    Name = "Destinations",
                    Value = dest
                });
            }
            source.Patch(Constants.RavenReplicationDestinations, patches.ToArray());
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

        protected Attachment WaitForAttachment(IDocumentStore store2, string expectedId, Etag changedSince = null)
        {
            Attachment attachment = null;

            for (int i = 0; i < RetriesCount; i++)
            {
                attachment = store2.DatabaseCommands.GetAttachment(expectedId);
                if (attachment != null)
                {
                    if (changedSince != null)
                    {
                        if (attachment.Etag != changedSince)
                            break;
                    }
                    else break;
                }
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

        protected void WaitForDocument(IDatabaseCommands commands, string expectedId, Etag afterEtag = null, CancellationToken? token = null)
        {
            if (afterEtag != null)
                throw new NotImplementedException();

            for (int i = 0; i < RetriesCount; i++)
            {
                if (token.HasValue)
                    token.Value.ThrowIfCancellationRequested();

                if (commands.Head(expectedId) != null)
                    break;
                Thread.Sleep(100);
            }

            var jsonDocumentMetadata = commands.Head(expectedId);

            Assert.NotNull(jsonDocumentMetadata);
        }

        protected bool WaitForDocument(IDatabaseCommands commands, string expectedId, int timeoutInMs)
        {
            var cts = new CancellationTokenSource();
            var waitingTask = Task.Run(() => WaitForDocument(commands, expectedId, null, cts.Token), cts.Token);

            Task.WaitAny(waitingTask, Task.Delay(timeoutInMs, cts.Token));

            cts.Cancel();
            return AsyncHelpers.RunSync(() => waitingTask.ContinueWith(t => commands.Head(expectedId) != null));
        }

        protected void WaitForReplication(IDocumentStore store, string id, string db = null, Etag changedSince = null)
        {
            for (int i = 0; i < RetriesCount; i++)
            {
                using (var session = store.OpenSession(db))
                {
                    var e = session.Load<object>(id);
                    if (e != null)
                    {
                        if (changedSince != null)
                        {
                            if (session.Advanced.GetEtagFor(e) != changedSince)
                                break;

                            Thread.Sleep(100);
                            continue;
                        }
                    }
                    else
                    {
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

        protected bool WaitForIndexToReplicate(IDatabaseCommands commands, string indexName, int timeoutInMilliseconds = 1500)
        {
            var mre = new ManualResetEventSlim();
            hasWaitEnded = false;
            Task.Run(() =>
            {
                while (hasWaitEnded == false)
                {
                    var stats = commands.GetStatistics();
                    if (stats.Indexes.Any(x => x.Name == indexName))
                    {
                        mre.Set();
                        break;
    }
                    Thread.Sleep(25);
}
            });

            var success = mre.Wait(timeoutInMilliseconds);
            hasWaitEnded = true;
            return success;
        }

        protected bool WaitForIndexDeletionToReplicate(IDatabaseCommands commands, string indexName, int timeoutInMilliseconds = 1500)
        {
            var mre = new ManualResetEventSlim();
            hasWaitEnded = false;
            Task.Run(() =>
            {
                while (hasWaitEnded == false)
                {
                    var stats = commands.GetStatistics();
                    if (stats.Indexes.Any(x => x.Name == indexName) == false)
                    {
                        mre.Set();
                        break;
                    }
                    Thread.Sleep(25);
                }
            });

            var success = mre.Wait(timeoutInMilliseconds);
            hasWaitEnded = true;
            return success;
        }

        protected bool WaitForIndexToReplicate(IAsyncDatabaseCommands commands, string indexName, int timeoutInMilliseconds = 1500)
        {
            var mre = new ManualResetEventSlim();
            hasWaitEnded = false;
            Task.Run(async () =>
            {
                while (hasWaitEnded == false)
                {
                    var stats = await commands.GetStatisticsAsync().ConfigureAwait(false);
                    if (stats.Indexes.Any(x => x.Name == indexName))
                    {
                        mre.Set();
                        break;
                    }
                    Thread.Sleep(25);
                }
            });

            var success = mre.Wait(timeoutInMilliseconds);
            hasWaitEnded = true;
            return success;
        }
    }
}
