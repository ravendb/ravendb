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
using Raven.Json.Linq;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Replication
{
    public class TransformerReplication : RavenTestBase
    {
        public class UserWithExtraInfo
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string Address { get; set; }
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class UserWithoutExtraInfoTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
        {
            public override string TransformerName { get { return "UserWithoutExtraInfoTransformer"; } }

            public UserWithoutExtraInfoTransformer()
            {
                TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
                                                         select new
                                                         {
                                                             u.Id,
                                                             u.Name
                                                         };
            }
        }

        public class UserWithoutExtraInfoTransformer_Extended : AbstractTransformerCreationTask<UserWithExtraInfo>
        {
            public override string TransformerName { get { return "UserWithoutExtraInfoTransformer"; } }

            public UserWithoutExtraInfoTransformer_Extended()
            {
                TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
                                                         select new
                                                         {
                                                             u.Id,
                                                             u.Name,
                                                             u.Address
                                                         };
            }
        }

        public class AnotherTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
        {
            public AnotherTransformer()
            {
                TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
                                                         select new
                                                         {
                                                             u.Id,
                                                             u.Name
                                                         };
            }
        }

        public class YetAnotherTransformer : AbstractTransformerCreationTask<UserWithExtraInfo>
        {
            public YetAnotherTransformer()
            {
                TransformResults = usersWithExtraInfo => from u in usersWithExtraInfo
                                                         select new
                                                         {
                                                             u.Id,
                                                             u.Name
                                                         };
            }
        }

        [Fact]
        public void Should_replicate_transformers_by_default()
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

                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                var transformer = new UserWithoutExtraInfoTransformer();
                transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

                var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

                var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));

                var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));
            }
        }

        [Fact]
        public async Task Should_replicate_transformer_deletion()
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
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                var transformer = new UserWithoutExtraInfoTransformer();
                transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

                var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
                var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
                replicationTask.TransformerReplication.TimeToWaitBeforeSendingDeletesOfTransformersToSiblings = TimeSpan.Zero;
                SpinWait.SpinUntil(() => replicationTask.TransformerReplication.Execute());

                var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

                var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));

                var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));

                //now delete the transformer at the source and verify that the deletion is replicated
                source.DatabaseCommands.ForDatabase("testDB").DeleteTransformer(transformer.TransformerName);
                SpinWait.SpinUntil(() => replicationTask.TransformerReplication.Execute());

                transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(0, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

                transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(0, transformersOnDestination2.Count(x => x.Name == transformer.TransformerName));

                transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(0, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));

            }
        }

        [Fact]
        public void Should_skip_transformer_replication_if_flag_is_set()
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

                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => destination2 == store, destination1, destination2, destination3);

                var conflictDocumentsTransformer = new RavenConflictDocumentsTransformer(); // #RavenDB-3981
                var transformer = new UserWithoutExtraInfoTransformer();
                transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

                var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

                var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer
                    .TransformerName)
                    .ToList();
                Assert.Equal(0, transformersOnDestination2.Count);

                var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));
            }
        }

        [Fact]
        public void Transformer_replication_should_respect_skip_replication_flag()
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

                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => store == destination2, destination1, destination2, destination3);

                var conflictDocumentsTransformer = new RavenConflictDocumentsTransformer(); // #RavenDB-3981
                var transformer = new UserWithoutExtraInfoTransformer();
                transformer.Execute(source.DatabaseCommands.ForDatabase("testDB"), source.Conventions);

                var transformersOnDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination1.Count(x => x.Name == transformer.TransformerName));

                var transformersOnDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer
                    .TransformerName)
                    .ToList();
                Assert.Equal(0, transformersOnDestination2.Count);

                var transformersOnDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024);
                Assert.Equal(1, transformersOnDestination3.Count(x => x.Name == transformer.TransformerName));
            }

        }

        [Fact]
        public async Task Should_replicate_all_transformers_periodically()
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

                var userTransformer = new UserWithoutExtraInfoTransformer();
                var anotherTransformer = new AnotherTransformer();
                var yetAnotherTransformer = new YetAnotherTransformer();
                var conflictDocumentsTransformer = new RavenConflictDocumentsTransformer(); // #RavenDB-3981

                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

                var sourceDB = await sourceServer.Server.GetDatabaseInternal("testDB");
                var replicationTask = sourceDB.StartupTasks.OfType<ReplicationTask>().First();
                SpinWait.SpinUntil(() => replicationTask.TransformerReplication.Execute());

                var expectedTransformerNames = new HashSet<string>
                {
                    userTransformer.TransformerName,
                    anotherTransformer.TransformerName,
                    yetAnotherTransformer.TransformerName
                };

                var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();

                Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination1));
                Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination2));
                Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination3));
            }
        }

        [Fact]
        public void Should_replicate_all_transformers_only_to_specific_destination_if_relevant_endpoint_is_hit()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //make sure replication is off for indexes/transformers
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                // ReSharper disable once AccessToDisposedClosure
                var destinationDocuments = SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                // index and transformer replication is forced if we are replicating for the first time, so replicating one document to bypass this
                ReplicateOneDummyDocument(source, destination1, destination2, destination3);

                var userTransformer = new UserWithoutExtraInfoTransformer();
                var anotherTransformer = new AnotherTransformer();
                var yetAnotherTransformer = new YetAnotherTransformer();
                var conflictDocumentsTransformer = new RavenConflictDocumentsTransformer(); // #RavenDB-3981

                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

                var expectedTransformerNames = new HashSet<string>
                {
                    userTransformer.TransformerName,
                    anotherTransformer.TransformerName,
                    yetAnotherTransformer.TransformerName
                };

                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all-to-destination", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.Write(RavenJObject.FromObject(destinationDocuments[1]));
                replicationRequest.ExecuteRequest();

                var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();

                Assert.Equal(0, transformerNamesAtDestination1.Count);
                Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination2));
                Assert.Equal(0, transformerNamesAtDestination3.Count);
            }
        }

        [Fact]
        public void Should_replicate_all_transformers_if_relevant_endpoint_is_hit()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //make sure replication is off for indexes/transformers
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                var userTransformer = new UserWithoutExtraInfoTransformer();
                var anotherTransformer = new AnotherTransformer();
                var yetAnotherTransformer = new YetAnotherTransformer();
                var conflictDocumentsTransformer = new RavenConflictDocumentsTransformer(); // #RavenDB-3981

                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

                var expectedTransformerNames = new List<string>()
                {
                    userTransformer.TransformerName,
                    anotherTransformer.TransformerName,
                    yetAnotherTransformer.TransformerName
                };
                expectedTransformerNames.Sort();

                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => false, destination1, destination2, destination3);

                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024)
                    .Where(x => x.Name != conflictDocumentsTransformer.TransformerName)
                    .Select(x => x.Name)
                    .ToList();
                transformerNamesAtDestination1.Sort();
                transformerNamesAtDestination2.Sort();
                transformerNamesAtDestination3.Sort();
                Assert.Equal(expectedTransformerNames, transformerNamesAtDestination1);
                Assert.Equal(expectedTransformerNames, transformerNamesAtDestination2);
                Assert.Equal(expectedTransformerNames, transformerNamesAtDestination3);
            }
        }

        [Fact]
        public void Replicate_all_transformers_should_respect_disable_replication_flag()
        {
            var requestFactory = new HttpRavenRequestFactory();
            using (var sourceServer = GetNewServer(8077))
            using (var source = NewRemoteDocumentStore(ravenDbServer: sourceServer, fiddler: true))
            using (var destinationServer1 = GetNewServer(8078))
            using (var destination1 = NewRemoteDocumentStore(ravenDbServer: destinationServer1, fiddler: true))
            using (var destinationServer2 = GetNewServer())
            using (var destination2 = NewRemoteDocumentStore(ravenDbServer: destinationServer2, fiddler: true))
            using (var destinationServer3 = GetNewServer(8081))
            using (var destination3 = NewRemoteDocumentStore(ravenDbServer: destinationServer3, fiddler: true))
            {
                CreateDatabaseWithReplication(source, "testDB");
                CreateDatabaseWithReplication(destination1, "testDB");
                CreateDatabaseWithReplication(destination2, "testDB");
                CreateDatabaseWithReplication(destination3, "testDB");

                //make sure replication is off for indexes/transformers
                source.Conventions.IndexAndTransformerReplicationMode = IndexAndTransformerReplicationMode.None;

                var userTransformer = new UserWithoutExtraInfoTransformer();
                var anotherTransformer = new AnotherTransformer();
                var yetAnotherTransformer = new YetAnotherTransformer();
                var conflictDocumentsTransformer = new RavenConflictDocumentsTransformer(); // #RavenDB-3981

                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(anotherTransformer.TransformerName, anotherTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(yetAnotherTransformer.TransformerName, yetAnotherTransformer.CreateTransformerDefinition());

                var expectedTransformerNames = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase)
                {
                    userTransformer.TransformerName,
                    anotherTransformer.TransformerName,
                    yetAnotherTransformer.TransformerName,
                };

                // ReSharper disable once AccessToDisposedClosure
                SetupReplication(source, "testDB", store => store == destination2, destination1, destination2, destination3);

                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                var transformerNamesAtDestination1 = destination1.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024).Where(x => x.Name != conflictDocumentsTransformer.TransformerName);
                var transformerNamesAtDestination2 = destination2.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024).Where(x => x.Name != conflictDocumentsTransformer.TransformerName);
                var transformerNamesAtDestination3 = destination3.DatabaseCommands.ForDatabase("testDB").GetTransformers(0, 1024).Where(x => x.Name != conflictDocumentsTransformer.TransformerName);

                Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination1.Select(x => x.Name).ToArray()));
                Assert.Equal(0, transformerNamesAtDestination2.Count());
                Assert.True(expectedTransformerNames.SetEquals(transformerNamesAtDestination3.Select(x => x.Name).ToArray()));
            }
        }

        [Fact]
        public void should_replicate_only_updated_transformer()
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

                var userTransformer = new UserWithoutExtraInfoTransformer();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());

                //replicating transformer from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                var updatedUserTransformer = new UserWithoutExtraInfoTransformer_Extended();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, updatedUserTransformer.CreateTransformerDefinition());

                var transformer = source.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.True(updatedUserTransformer.CreateTransformerDefinition().TransformResults.Equals(transformer.TransformResults));

                //replicating transformer from the destination
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", destination.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = destination.Url
                });
                replicationRequest.ExecuteRequest();

                //the new transformer shouldn't be overwritten
                transformer = source.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.True(updatedUserTransformer.CreateTransformerDefinition().TransformResults.Equals(transformer.TransformResults));
            }
        }

        [Fact]
        public void can_update_transformer_lock_mode()
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

                var userTransformer = new UserWithoutExtraInfoTransformer();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                source.DatabaseCommands.ForDatabase("testDB").SetTransformerLock(userTransformer.TransformerName, TransformerLockMode.LockedIgnore);

                //replicating transformer from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                var updatedUserTransformer = new UserWithoutExtraInfoTransformer_Extended();
                source.DatabaseCommands.ForDatabase("testDB").SetTransformerLock(updatedUserTransformer.TransformerName, TransformerLockMode.Unlock);
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, updatedUserTransformer.CreateTransformerDefinition());

                var transformer = source.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.True(updatedUserTransformer.CreateTransformerDefinition().TransformResults.Equals(transformer.TransformResults));

                //replicating transformer from the source
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethods.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                //the transformer lock mode should change
                transformer = destination.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.Equal(transformer.LockMode, TransformerLockMode.Unlock);
                Assert.True(updatedUserTransformer.CreateTransformerDefinition().TransformResults.Equals(transformer.TransformResults));
            }
        }

        [Fact]
        public void should_ignore_outdated_transformer_delete()
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

                var userTransformer = new UserWithoutExtraInfoTransformer();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                destination.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                // transformer version = 1 on both servers

                var extendedTransformer = new UserWithoutExtraInfoTransformer_Extended();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, extendedTransformer.CreateTransformerDefinition());
                // transformer version = 2 on 'source server'

                destination.DatabaseCommands.ForDatabase("testDB").DeleteTransformer(userTransformer.TransformerName);
                // deleted transformer version = 1

                // replicating transformers from the destination
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", destination.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = destination.Url
                });
                replicationRequest.ExecuteRequest();

                // the transformer shouldn't be deleted
                var transformer = source.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.NotNull(transformer);
                Assert.True(extendedTransformer.CreateTransformerDefinition().Equals(transformer));

                // replicating transformers from the source
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                transformer = destination.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.NotNull(transformer);
                Assert.True(extendedTransformer.CreateTransformerDefinition().Equals(transformer));
            }
        }

        [Fact]
        public void should_accept_updated_transformer_delete()
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

                var userTransformer = new UserWithoutExtraInfoTransformer();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                destination.DatabaseCommands.ForDatabase("testDB").PutTransformer(userTransformer.TransformerName, userTransformer.CreateTransformerDefinition());
                // transformer version = 1 on both servers

                var extendedUserTransformer = new UserWithoutExtraInfoTransformer_Extended();
                source.DatabaseCommands.ForDatabase("testDB").PutTransformer(extendedUserTransformer.TransformerName, extendedUserTransformer.CreateTransformerDefinition());
                // transformer version = 2 on 'source server'

                source.DatabaseCommands.ForDatabase("testDB").DeleteTransformer(userTransformer.TransformerName);
                // deleted transformer version = 2 on 'source server'

                // replicating transformers from the source
                var replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", source.Url);
                var replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = source.Url
                });
                replicationRequest.ExecuteRequest();

                // the transformer should be deleted
                var transformer = destination.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.Null(transformer);

                // replicating transformers from the destination
                replicationRequestUrl = string.Format("{0}/databases/testDB/replication/replicate-transformers?op=replicate-all", destination.Url);
                replicationRequest = requestFactory.Create(replicationRequestUrl, HttpMethod.Post, new RavenConnectionStringOptions
                {
                    Url = destination.Url
                });
                replicationRequest.ExecuteRequest();

                transformer = source.DatabaseCommands.ForDatabase("testDB").GetTransformer(userTransformer.TransformerName);
                Assert.Null(transformer);
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

        private void ReplicateOneDummyDocument(IDocumentStore source, IDocumentStore destination1, IDocumentStore destination2, IDocumentStore destination3)
        {
            string id;
            using (var session = source.OpenSession("testDB"))
            {
                var dummy = new { Id = "" };

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
