using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Bundles.Replication;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Replication
{
    public class IndexReplication : ReplicationBase
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "Replication";
        }

        public class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string LastName { get; set; }
        }

        public class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }

        public class UserIndex_Extended : AbstractIndexCreationTask<User>
        {
            public override string IndexName { get { return "UserIndex"; } }

            public UserIndex_Extended()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.LastName
                               };
            }
        }

        public class AnotherUserIndex : AbstractIndexCreationTask<User>
        {
            public AnotherUserIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }

        public class YetAnotherUserIndex : AbstractIndexCreationTask<User>
        {
            public YetAnotherUserIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }

        private static void SetupReplication(IDocumentStore source, string databaseName, params IDocumentStore[] destinations)
        {
            source
                .DatabaseCommands
                .ForDatabase(databaseName)
                .Put(
                    Constants.RavenReplicationDestinations,
                    null,
                    RavenJObject.FromObject(new ReplicationDocument
                    {
                        Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
                            new ReplicationDestination
                            {
                                Database = databaseName,
                                Url = destination.Url
                            }))

                    }),
                    new RavenJObject());
        }

        private static List<ReplicationDestination> SetupReplication(IDocumentStore source, string databaseName, Func<IDocumentStore, bool> shouldSkipIndexReplication, params IDocumentStore[] destinations)
        {
            var replicationDocument = new ReplicationDocument
            {
                Destinations = new List<ReplicationDestination>(destinations.Select(destination =>
                    new ReplicationDestination
                    {
                        Database = databaseName,
                        Url = destination.Url,
                        SkipIndexReplication = shouldSkipIndexReplication(destination)
                    }))
            };

            using (var session = source.OpenSession(databaseName))
            {
                session.Store(replicationDocument, Constants.RavenReplicationDestinations);
                session.SaveChanges();
            }

            return replicationDocument.Destinations;
        }

        [Fact]
        public async Task Should_replicate_indexes_periodically()
        {
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                var anotherUserIndex = new AnotherUserIndex();
                var yetAnotherUserIndex = new YetAnotherUserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(anotherUserIndex.IndexName, anotherUserIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(yetAnotherUserIndex.IndexName, yetAnotherUserIndex.CreateIndexDefinition());

                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);


                var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
                var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
                SpinWait.SpinUntil(() => replicationTask.IndexReplication.Execute());

                var expectedIndexNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) {userIndex.IndexName, anotherUserIndex.IndexName, yetAnotherUserIndex.IndexName, ConflictDocumentsIndex};
                var indexStatsAfterReplication1 = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name);
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication1));

                var indexStatsAfterReplication3 = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name);
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication3));

                var indexStatsAfterReplication2 = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name);
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication2));
            }
        }

        [Fact]
        public async Task Should_send_last_queried_index_time_periodically()
        {
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                var anotherUserIndex = new AnotherUserIndex();
                var yetAnotherUserIndex = new YetAnotherUserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(anotherUserIndex.IndexName, anotherUserIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(yetAnotherUserIndex.IndexName, yetAnotherUserIndex.CreateIndexDefinition());

                using (var session = source.OpenSession("testDB"))
                {
                    // ReSharper disable ReturnValueOfPureMethodIsNotUsed
                    session.Query<UserIndex>(userIndex.IndexName).ToList(); //update last queried time
                    session.Query<AnotherUserIndex>(anotherUserIndex.IndexName).ToList(); //update last queried time
                    session.Query<YetAnotherUserIndex>(yetAnotherUserIndex.IndexName).ToList(); //update last queried time
                    // ReSharper restore ReturnValueOfPureMethodIsNotUsed

                    session.SaveChanges();
                }

                var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
                var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
                replicationTask.IndexReplication.SendLastQueried();

                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);

                var indexNames = new[] {userIndex.IndexName, anotherUserIndex.IndexName, yetAnotherUserIndex.IndexName};

                var sourceIndexStats = source.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => indexNames.Contains(x.Name)).ToList();
                var destination1IndexStats = source.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => indexNames.Contains(x.Name)).ToList();
                var destination2IndexStats = source.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => indexNames.Contains(x.Name)).ToList();
                var destination3IndexStats = source.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => indexNames.Contains(x.Name)).ToList();

                Assert.NotNull(sourceIndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp); //sanity check
                Assert.NotNull(sourceIndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp); //sanity check
                Assert.NotNull(sourceIndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp); //sanity check

                Assert.Equal(sourceIndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp, destination1IndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp);
                Assert.Equal(sourceIndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp, destination1IndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp);
                Assert.Equal(sourceIndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp, destination1IndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp);

                Assert.Equal(sourceIndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp, destination2IndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp);
                Assert.Equal(sourceIndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp, destination2IndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp);
                Assert.Equal(sourceIndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp, destination2IndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp);

                Assert.Equal(sourceIndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp, destination3IndexStats.First(x => x.Name == userIndex.IndexName).LastQueryTimestamp);
                Assert.Equal(sourceIndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp, destination3IndexStats.First(x => x.Name == anotherUserIndex.IndexName).LastQueryTimestamp);
                Assert.Equal(sourceIndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp, destination3IndexStats.First(x => x.Name == yetAnotherUserIndex.IndexName).LastQueryTimestamp);

            }
        }

        [Fact]
        public void Should_replicate_all_indexes_if_relevant_endpoint_is_hit()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8076))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer1 = GetNewServer(8077))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
            using (var destinationServer2 = GetNewServer(8078))
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
            using (var destinationServer3 = GetNewServer(8079))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                // index replication can be triggered if we haven't replicated anything yet, bypassing this
                source.DatabaseCommands.ForDatabase("testDB").Put("keys/1", null, new RavenJObject(), new RavenJObject());
                WaitForDocument(destination1.DatabaseCommands.ForDatabase("testDB"), "keys/1");
                WaitForDocument(destination2.DatabaseCommands.ForDatabase("testDB"), "keys/1");
                WaitForDocument(destination3.DatabaseCommands.ForDatabase("testDB"), "keys/1");

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                var anotherUserIndex = new AnotherUserIndex();
                var yetAnotherUserIndex = new YetAnotherUserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(anotherUserIndex.IndexName, anotherUserIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(yetAnotherUserIndex.IndexName, yetAnotherUserIndex.CreateIndexDefinition());

                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });

                replicationRequest.ExecuteRequest();

                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);

                var expectedIndexNames = new List<string> {userIndex.IndexName, anotherUserIndex.IndexName, yetAnotherUserIndex.IndexName, ConflictDocumentsIndex};
                expectedIndexNames.Sort(StringComparer.InvariantCultureIgnoreCase);
                var indexStatsAfterReplication1 = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToList();
                indexStatsAfterReplication1.Sort(StringComparer.InvariantCultureIgnoreCase);
                Assert.Equal(expectedIndexNames, indexStatsAfterReplication1);


                var indexStatsAfterReplication3 = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToList();
                indexStatsAfterReplication3.Sort(StringComparer.InvariantCultureIgnoreCase);
                Assert.Equal(expectedIndexNames, indexStatsAfterReplication3);

                var indexStatsAfterReplication2 = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToList();
                indexStatsAfterReplication2.Sort(StringComparer.InvariantCultureIgnoreCase);
                Assert.Equal(expectedIndexNames, indexStatsAfterReplication2);


            }
        }

        [Fact]
        public void Should_replicate_all_indexes_only_to_specific_destination_if_relevant_endpoint_hit()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                // ReSharper disable once AccessToDisposedClosure
                var destinationDocuments = SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                // index and transformer replication is forced if we are replicating for the first time, so replicating one document to bypass this
                ReplicateOneDummyDocument(source, destination1, destination2, destination3);

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                var anotherUserIndex = new AnotherUserIndex();
                var yetAnotherUserIndex = new YetAnotherUserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(anotherUserIndex.IndexName, anotherUserIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(yetAnotherUserIndex.IndexName, yetAnotherUserIndex.CreateIndexDefinition());

                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all-to-destination", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });

                replicationRequest.Write(RavenJObject.FromObject(destinationDocuments[1]));
                replicationRequest.ExecuteRequest();

                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName);
                WaitForIndexToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName);

                var expectedIndexNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) {userIndex.IndexName, anotherUserIndex.IndexName, yetAnotherUserIndex.IndexName, ConflictDocumentsIndex};
                var indexStatsAfterReplication1 = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => x.Name != ConflictDocumentsIndex);
                var indexStatsAfterReplication2 = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name);
                var indexStatsAfterReplication3 = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => x.Name != ConflictDocumentsIndex);

                Assert.Equal(0, indexStatsAfterReplication1.Count());
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication2));
                Assert.Equal(0, indexStatsAfterReplication3.Count());
            }
        }

        private const string ConflictDocumentsIndex = "Raven/ConflictDocuments";

        [Fact]
        public void Replicate_all_indexes_should_respect_disable_indexing_flag()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(9077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer1 = GetNewServer(9078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
            using (var destinationServer3 = GetNewServer(9081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                var sourceReplicationTask = sourceServer.Options.DatabaseLandlord
                    .GetResourceInternal("testDB").Result
                    .StartupTasks
                    .OfType<ReplicationTask>()
                    .First();

                sourceReplicationTask.Pause();

                // ReSharper disable once AccessToDisposedClosure
                //we setup replication so replication to destination2 will have "disabled" flag
                SetupReplication(source, "testDB", store => store == destination2, destination1, destination2, destination3);

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                var anotherUserIndex = new AnotherUserIndex();
                var yetAnotherUserIndex = new YetAnotherUserIndex();
                var conflictIndex = new RavenConflictDocuments();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(anotherUserIndex.IndexName, anotherUserIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(yetAnotherUserIndex.IndexName, yetAnotherUserIndex.CreateIndexDefinition());

                sourceReplicationTask.Continue();

                var replicationRequestUrl = $"{source.Url}/databases/testDB/replication/replicate-indexes?op=replicate-all";
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                Assert.True(WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName));
                Assert.True(WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName));
                Assert.True(WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName));

                var expectedIndexNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) {conflictIndex.IndexName, userIndex.IndexName, anotherUserIndex.IndexName, yetAnotherUserIndex.IndexName};
                var indexStatsAfterReplication1 = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToList();
                Assert.Equal(expectedIndexNames, indexStatsAfterReplication1);



                Assert.True(WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName));
                Assert.True(WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), anotherUserIndex.IndexName));
                Assert.True(WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), yetAnotherUserIndex.IndexName));
                var indexStatsAfterReplication3 = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToList();
                Assert.Equal(expectedIndexNames, indexStatsAfterReplication3);


                //since destination2 has disabled flag - indexes should not replicate to here
                var expectedConflitDocumentsOnlyName = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) {conflictIndex.IndexName};
                var indexStatsAfterReplication2 = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToList();
                Assert.Equal(expectedConflitDocumentsOnlyName, indexStatsAfterReplication2);
            }
        }

        [Fact]
        public void Should_skip_index_replication_if_serverside_flag_is_true()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => store == destination2, destination1, destination2, destination3);

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());

                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replication&indexName={1}", source.Url, userIndex.IndexName);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                WaitForIndexToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);

                var indexStatsAfterReplication = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.True(indexStatsAfterReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));

                //this one should not have replicated index -> because of SkipIndexReplication = true in ReplicationDocument of source
                indexStatsAfterReplication = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.False(indexStatsAfterReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));

                indexStatsAfterReplication = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.True(indexStatsAfterReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));
            }
        }

        [Fact]
        public void ExecuteIndex_should_replicate_indexes_by_default()
        {
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                SetupReplication(source, "testDB", destination);

                var userIndex = new UserIndex();

                var indexStatsBeforeReplication = destination.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.False(indexStatsBeforeReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));

                //this should fire http request to index replication endpoint -> so the index is replicated
                userIndex.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

                WaitForIndexToReplicate(destination.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);

                var indexStatsAfterReplication = destination.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.True(indexStatsAfterReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));
            }
        }


        [Fact]
        public void CanReplicateIndex()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                var userIndex = new UserIndex();

                SetupReplication(source, "testDB", destination);

                var indexStatsBeforeReplication = destination.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.False(indexStatsBeforeReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));

                //make sure not to replicate the index automatically
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());


                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?indexName={1}", source.Url, userIndex.IndexName);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                WaitForIndexToReplicate(destination.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);

                var indexStatsAfterReplication = destination.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes;
                Assert.True(indexStatsAfterReplication.Any(index => index.Name.Equals(userIndex.IndexName, StringComparison.InvariantCultureIgnoreCase)));
            }
        }

        [Fact]
        public async Task Should_replicate_index_deletion()
        {
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3))
            {


                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //turn-off automatic index replication - precaution
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                //make sure not to replicate the index automatically
                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());

                var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
                var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
                replicationTask.IndexReplication.TimeToWaitBeforeSendingDeletesOfIndexesToSiblings = TimeSpan.Zero;
                SpinWait.SpinUntil(() => replicationTask.IndexReplication.Execute());

                var expectedIndexNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) {userIndex.IndexName, ConflictDocumentsIndex};
                var indexStatsAfterReplication1 = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToArray();
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication1));

                var indexStatsAfterReplication3 = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToArray();
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication3));

                var indexStatsAfterReplication2 = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Select(x => x.Name).ToArray();
                Assert.True(expectedIndexNames.SetEquals(indexStatsAfterReplication2));

                source.DatabaseCommands.ForDatabase("testDB").DeleteIndex(userIndex.IndexName);

                //the index is now replicated on all servers.
                //now delete the index and verify that deletion is replicated
                SpinWait.SpinUntil(() => replicationTask.IndexReplication.Execute());

                WaitForIndexDeletionToReplicate(destination1.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexDeletionToReplicate(destination2.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);
                WaitForIndexDeletionToReplicate(destination3.DatabaseCommands.ForDatabase("testDB"), userIndex.IndexName);


                indexStatsAfterReplication1 = destination1.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => x.Name != ConflictDocumentsIndex).Select(x => x.Name).ToArray();
                Assert.Empty(indexStatsAfterReplication1);

                indexStatsAfterReplication2 = destination2.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => x.Name != ConflictDocumentsIndex).Select(x => x.Name).ToArray();
                Assert.Empty(indexStatsAfterReplication2);

                indexStatsAfterReplication3 = destination3.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Where(x => x.Name != ConflictDocumentsIndex).Select(x => x.Name).ToArray();
                Assert.Empty(indexStatsAfterReplication3);
            }
        }

        [Fact]
        public void should_replicate_only_updated_indexes()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                destination.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                SetupReplication(source, "testDB", store => false, destination);
                SetupReplication(destination, "testDB", store => false, source);

                for (var i = 0; i < 30; i++)
                {
                    //just for starting the initial index and transformer replication
                    source.DatabaseCommands.ForDatabase("testDB").Put("test" + i, Etag.Empty, new RavenJObject(), new RavenJObject());
                    destination.DatabaseCommands.ForDatabase("testDB").Put("test" + (i + 50), Etag.Empty, new RavenJObject(), new RavenJObject());
                }

                WaitForDocument(destination.DatabaseCommands.ForDatabase("testDB"), "test29");
                WaitForDocument(source.DatabaseCommands.ForDatabase("testDB"), "test79");

                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());

                //replicating indexes from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                var updatedUserIndex = new UserIndex_Extended();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, updatedUserIndex.CreateIndexDefinition(), true);

                var index = source.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.True(updatedUserIndex.CreateIndexDefinition().Map.Equals(index.Map));

                //replicating indexes from the destination
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", destination.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = destination.Url
                });
                replicationRequest.ExecuteRequest();

                //the new index shouldn't be overwritten
                index = source.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.True(updatedUserIndex.CreateIndexDefinition().Map.Equals(index.Map));
            }
        }

        [Fact]
        public void can_update_index_lock_mode()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                destination.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                SetupReplication(source, "testDB", store => false, destination);
                SetupReplication(destination, "testDB", store => false, source);

                for (var i = 0; i < 30; i++)
                {
                    //just for starting the initial index and transformer replication
                    source.DatabaseCommands.ForDatabase("testDB").Put("test" + i, Etag.Empty, new RavenJObject(), new RavenJObject());
                    destination.DatabaseCommands.ForDatabase("testDB").Put("test" + (i + 50), Etag.Empty, new RavenJObject(), new RavenJObject());
                }

                WaitForDocument(destination.DatabaseCommands.ForDatabase("testDB"), "test29");
                WaitForDocument(source.DatabaseCommands.ForDatabase("testDB"), "test79");

                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                source.DatabaseCommands.ForDatabase("testDB").SetIndexLock(userIndex.IndexName, IndexLockMode.LockedIgnore);

                //replicating index from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                var updatedUserIndex = new UserIndex_Extended();
                source.DatabaseCommands.ForDatabase("testDB").SetIndexLock(updatedUserIndex.IndexName, IndexLockMode.Unlock);
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(updatedUserIndex.IndexName, updatedUserIndex.CreateIndexDefinition(), true);

                var index = source.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.True(updatedUserIndex.CreateIndexDefinition().Map.Equals(index.Map));

                //replicating index from the source
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                //the index lock mode should change
                index = destination.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.Equal(index.LockMode, IndexLockMode.Unlock);
                Assert.True(updatedUserIndex.CreateIndexDefinition().Map.Equals(index.Map));
            }
        }

        [Fact]
        public void can_replicate_index_when_lock_mode_is_error()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                destination.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                SetupReplication(source, "testDB", store => false, destination);
                SetupReplication(destination, "testDB", store => false, source);

                for (var i = 0; i < 30; i++)
                {
                    //just for starting the initial index and transformer replication
                    source.DatabaseCommands.ForDatabase("testDB").Put("test" + i, Etag.Empty, new RavenJObject(), new RavenJObject());
                    destination.DatabaseCommands.ForDatabase("testDB").Put("test" + (i + 50), Etag.Empty, new RavenJObject(), new RavenJObject());
                }

                WaitForDocument(destination.DatabaseCommands.ForDatabase("testDB"), "test29");
                WaitForDocument(source.DatabaseCommands.ForDatabase("testDB"), "test79");

                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());

                //replicating indexes from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                //only Raven/ByEntityName
                Assert.Equal(2, destination.DatabaseCommands.GetStatistics().Indexes.Length);

                source.DatabaseCommands.ForDatabase("testDB").SetIndexLock(userIndex.IndexName, IndexLockMode.LockedError);

                var userIndex2 = new AnotherUserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex2.IndexName, userIndex2.CreateIndexDefinition());

                //replicating indexes from the source
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                //the new index should be replicated
                Assert.Equal(3, destination.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Length);
            }
        }

        [Fact]
        public void can_replicate_index_when_lock_mode_is_side_by_side()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                destination.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                SetupReplication(source, "testDB", store => false, destination);
                SetupReplication(destination, "testDB", store => false, source);

                for (var i = 0; i < 30; i++)
                {
                    //just for starting the initial index and transformer replication
                    source.DatabaseCommands.ForDatabase("testDB").Put("test" + i, Etag.Empty, new RavenJObject(), new RavenJObject());
                    destination.DatabaseCommands.ForDatabase("testDB").Put("test" + (i + 50), Etag.Empty, new RavenJObject(), new RavenJObject());
                }

                WaitForDocument(destination.DatabaseCommands.ForDatabase("testDB"), "test29");
                WaitForDocument(source.DatabaseCommands.ForDatabase("testDB"), "test79");

                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());

                //replicating indexes from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                //only Raven/ByEntityName
                Assert.Equal(2, destination.DatabaseCommands.GetStatistics().Indexes.Length);

                source.DatabaseCommands.ForDatabase("testDB").SetIndexLock(userIndex.IndexName, IndexLockMode.SideBySide);

                var userIndex2 = new AnotherUserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex2.IndexName, userIndex2.CreateIndexDefinition());

                //replicating indexes from the source
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                //the new index should be replicated
                Assert.Equal(3, destination.DatabaseCommands.ForDatabase("testDB").GetStatistics().Indexes.Length);
            }
        }

        [Fact]
        public void should_ignore_outdated_index_delete()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                destination.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                SetupReplication(source, "testDB", store => false, destination);
                SetupReplication(destination, "testDB", store => false, source);

                for (var i = 0; i < 30; i++)
                {
                    //just for starting the initial index and transformer replication
                    source.DatabaseCommands.ForDatabase("testDB").Put("test" + i, Etag.Empty, new RavenJObject(), new RavenJObject());
                    destination.DatabaseCommands.ForDatabase("testDB").Put("test" + (i + 50), Etag.Empty, new RavenJObject(), new RavenJObject());
                }

                WaitForDocument(destination.DatabaseCommands.ForDatabase("testDB"), "test29");
                WaitForDocument(source.DatabaseCommands.ForDatabase("testDB"), "test79");

                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                destination.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                // index version = 1 on both servers

                var extendedUserIndex = new UserIndex_Extended();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(extendedUserIndex.IndexName, extendedUserIndex.CreateIndexDefinition(), true);
                // index version = 2 on 'source server'

                destination.DatabaseCommands.ForDatabase("testDB").DeleteIndex(userIndex.IndexName);
                // deleted index version = 1

                // replicating indexes from the destination
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", destination.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = destination.Url
                });
                replicationRequest.ExecuteRequest();

                // the index shouldn't be deleted
                var index = source.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.NotNull(index);
                Assert.True(extendedUserIndex.CreateIndexDefinition().Map.Equals(index.Map));

                // replicating indexes from the source
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                index = destination.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.NotNull(index);
                Assert.True(extendedUserIndex.CreateIndexDefinition().Map.Equals(index.Map));
            }
        }

        [Fact]
        public void should_accept_updated_index_delete()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer = GetNewServer(8078))
            using (var destination = NewRemoteDocumentStore(ravenDbServer: destinationServer, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination, "testDB");

                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;
                destination.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                SetupReplication(source, "testDB", store => false, destination);
                SetupReplication(destination, "testDB", store => false, source);

                for (var i = 0; i < 30; i++)
                {
                    //just for starting the initial index and transformer replication
                    source.DatabaseCommands.ForDatabase("testDB").Put("test" + i, Etag.Empty, new RavenJObject(), new RavenJObject());
                    destination.DatabaseCommands.ForDatabase("testDB").Put("test" + (i + 50), Etag.Empty, new RavenJObject(), new RavenJObject());
                }

                WaitForDocument(destination.DatabaseCommands.ForDatabase("testDB"), "test29");
                WaitForDocument(source.DatabaseCommands.ForDatabase("testDB"), "test79");

                var userIndex = new UserIndex();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                destination.DatabaseCommands.ForDatabase("testDB").PutIndex(userIndex.IndexName, userIndex.CreateIndexDefinition());
                // index version = 1 on both servers

                var extendedUserIndex = new UserIndex_Extended();
                source.DatabaseCommands.ForDatabase("testDB").PutIndex(extendedUserIndex.IndexName, extendedUserIndex.CreateIndexDefinition(), true);
                // index version = 2 on 'source server'

                source.DatabaseCommands.ForDatabase("testDB").DeleteIndex(userIndex.IndexName);
                // deleted index version = 2 on 'source server'

                // replicating indexes from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                // the index should be deleted
                var index = destination.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.Null(index);

                // replicating indexes from the destination
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-indexes?op=replicate-all", destination.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = destination.Url
                });
                replicationRequest.ExecuteRequest();

                index = source.DatabaseCommands.ForDatabase("testDB").GetIndex(userIndex.IndexName);
                Assert.Null(index);
            }
        }

        private static void CreateDatabaseWithReplication(DocumentStore store, string databaseName)
        {
            store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
            {
                Id = databaseName,
                Settings =
                {
                    {"Raven/DataDir", "~/Tenants/" + databaseName},
                    {"Raven/ActiveBundles", "Replication"}
                }
            });
        }

        private void ReplicateOneDummyDocument(IDocumentStore source, IDocumentStore destination1, IDocumentStore destination2, IDocumentStore destination3)
        {
            string id;
            using (var session = source.OpenSession("testDB"))
            {
                var dummy = new {Id = ""};

                session.Store(dummy);
                session.SaveChanges();

                id = dummy.Id;
            }

            WaitForDocument(destination1.DatabaseCommands.ForDatabase("testDB"), id);
            WaitForDocument(destination2.DatabaseCommands.ForDatabase("testDB"), id);
            WaitForDocument(destination3.DatabaseCommands.ForDatabase("testDB"), id);
        }
    }
}
