using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Bundles.Replication.Tasks;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3573 : ReplicationBase
    {
        public class Foo
        {
            public string Id { get; set; }

            public int Bar { get; set; }
        }

        public class User
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        public class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name,
                        Date = new DateTime(2015,1,1)
                    };
            }
        }

        public class UserIndex2 : AbstractIndexCreationTask<User>
        {
            private string indexName;
            public override string IndexName
            {
                get { return indexName; }
            }

            public UserIndex2()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   Date1 = new DateTime(2015, 1, 1),
                                   Date2 = new DateTime(1998, 1, 1),
                               };
            }

            public void SetName(string name)
            {
                indexName = name;
            }
        }

        [Fact]
        public async Task Side_by_side_index_should_be_replicated()
        {
            using(var source = CreateStore())
            using (var destination = CreateStore())
            {
                var testIndex = new UserIndex();
                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };
                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);

                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();

                SetupReplication(source.DatabaseCommands, destination);
                sourceReplicationTask.IndexReplication.Execute(); //now the old index replicated to destination

                var sideBySideIndexReplicated = new ManualResetEventSlim();
                var replaceIndexName = Constants.SideBySideIndexNamePrefix + testIndex.IndexName;
                destinationDatabase.Notifications.OnIndexChange += (database, notification) =>
                {
                    if (notification.Type == IndexChangeTypes.IndexAdded &&
                        notification.Name.Equals(replaceIndexName))
                        sideBySideIndexReplicated.Set();
                };

                testIndex.SideBySideExecute(source);

                sourceReplicationTask.IndexReplication.Execute();

                Assert.True(sideBySideIndexReplicated.Wait(2000));

                var definition = destination.DatabaseCommands.GetIndex(replaceIndexName);
                Assert.NotNull(definition);
                Assert.True(definition.Equals(testIndex.CreateIndexDefinition(),false));

                var replacementIndexName = Constants.SideBySideIndexNamePrefix + testIndex.IndexName;
                VerifyReplacementDocumentIsThere(replacementIndexName, destinationDatabase);
            }
        }

        [Fact]
        public async Task If_deleted_original_index_on_destination_should_simply_create_the_replacement_index()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);

                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                var testIndex = new UserIndex();
                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };
                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                using (var session = source.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                        session.Store(new User
                        {
                            Name = "User - " + i
                        });

                    session.SaveChanges();
                }

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();

                SetupReplication(source.DatabaseCommands, destination);

                testIndex.SideBySideExecute(source);

                //the side by side will be automatically replicated and saved as a simple index
                Assert.Null(destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName));

                var definition = destination.DatabaseCommands.GetIndex(testIndex.IndexName);
                Assert.NotNull(definition);
                Assert.True(definition.Equals(testIndex.CreateIndexDefinition(), false));
            }
        }

        [Fact]
        public async Task If_original_index_exists_but_no_side_by_side_index_then_create_side_by_side_index()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var testIndex = new UserIndex();
                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };
                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);

                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();

                SetupReplication(source.DatabaseCommands, destination);

                //replicate the original index
                sourceReplicationTask.IndexReplication.Execute();

                testIndex.SideBySideExecute(source);

                //do side-by-side index replication -> since in the destination there is original index, 
                //simply create the side-by-side index as so it will replace the original when it catches up
                sourceReplicationTask.IndexReplication.Execute();

                var originalDefinition = destination.DatabaseCommands.GetIndex(testIndex.IndexName);
                Assert.NotNull(originalDefinition);
                Assert.True(originalDefinition.Equals(oldIndexDef, false));

                var sideBySideDefinition = destination.DatabaseCommands.GetIndex(Constants.SideBySideIndexNamePrefix + testIndex.IndexName);
                Assert.NotNull(sideBySideDefinition);
                Assert.True(sideBySideDefinition.Equals(testIndex.CreateIndexDefinition(), false));
            }
        }

        [Fact]
        public async Task Side_by_side_index_after_replication_should_have_appropriate_minimum_etag_for_the_destination_if_applicable()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var testIndex = new UserIndex();
                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };
                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);

                using (var session = source.OpenSession())
                {
                    for(int i = 0; i < 10; i++)
                        session.Store(new User
                        {
                            Name = "User - " + i
                        });

                    session.SaveChanges();
                }

                WaitForIndexing(source);
                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();

                SetupReplication(source.DatabaseCommands, destination);

                //replicate the original index
                await sourceReplicationTask.ExecuteReplicationOnce(true);
                sourceReplicationTask.IndexReplication.Execute();

                destinationDatabase.SpinBackgroundWorkers();

                WaitForIndexing(destination);

                destinationDatabase.StopBackgroundWorkers();

                testIndex.SideBySideExecute(source,sourceDatabase.Statistics.LastDocEtag);

                var replacementIndexName = Constants.SideBySideIndexNamePrefix + testIndex.IndexName;

                //do side-by-side index replication -> since in the destination there is original index, 
                //simply create the side-by-side index as so it will replace the original when it catches up
                sourceReplicationTask.IndexReplication.Execute();

                var originalDefinition = destination.DatabaseCommands.GetIndex(testIndex.IndexName);
                Assert.NotNull(originalDefinition);
                Assert.True(originalDefinition.Equals(oldIndexDef, false));

                var sideBySideDefinition = destination.DatabaseCommands.GetIndex(replacementIndexName);
                Assert.NotNull(sideBySideDefinition);
                Assert.True(sideBySideDefinition.Equals(testIndex.CreateIndexDefinition(), false));

                VerifyReplacementDocumentIsThere(replacementIndexName, destinationDatabase, true);
            }
        }

        [Fact]
        public async Task Out_of_date_side_by_side_index_will_get_updated_on_replication()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var testIndex = new UserIndex();
                var testIndex2 = new UserIndex2();
                testIndex2.SetName(testIndex.IndexName);

                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };

                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);

                using (var session = source.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Doe"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(source);
                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();

                SetupReplication(source.DatabaseCommands, destination);

                sourceReplicationTask.IndexReplication.Execute(); //replicate the usual index

                source.SideBySideExecuteIndex(testIndex);

                sourceReplicationTask.IndexReplication.Execute(); //replicate the side-by-side index

                //sanity check
                Assert.NotNull(destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName));

                sourceDatabase.Indexes.DeleteIndex(Constants.SideBySideIndexNamePrefix + testIndex.IndexName);
                
                source.SideBySideExecuteIndex(testIndex2); //replaces the testIndex side-by-side index on source

                sourceReplicationTask.IndexReplication.Execute(); //should replicate the replaced side-by-sude index to destination

                var sideBySideIndex = destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName);
                Assert.NotNull(sideBySideIndex);
                Assert.True(sideBySideIndex.Equals(testIndex2.CreateIndexDefinition(),false));
            }
        }

        /*
         * This tests the following scenario
         * 1) Index + Side-by-Side index replicated to destination
         * 2) Original index is deleted at destination, side-by-side index remains
         * 3) Index + Side-by-Side index replicated to destination again
         * 4) Result: The original index is recreated with side-by-side definition; The remaining side-by-side index on destination is deleted
         */
        [Fact]
        public async Task If_deleted_original_index_on_destination_but_not_side_by_side_index()
        {
            using (var source = CreateStore())
            using (var destination = CreateStore())
            {
                var sourceDatabase = await servers[0].Server.GetDatabaseInternal(source.DefaultDatabase);
                var destinationDatabase = await servers[1].Server.GetDatabaseInternal(destination.DefaultDatabase);
                sourceDatabase.StopBackgroundWorkers();
                destinationDatabase.StopBackgroundWorkers();

                var testIndex = new UserIndex();

                var oldIndexDef = new IndexDefinition
                {
                    Map = "from user in docs.Users\n select new {\n\tName = user.Name\n}"
                };

                source.DatabaseCommands.PutIndex(testIndex.IndexName, oldIndexDef);

                using (var session = source.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Doe"
                    });
                    session.SaveChanges();
                }

                var sourceReplicationTask = sourceDatabase.StartupTasks.OfType<ReplicationTask>().First();
                sourceReplicationTask.Pause();

                SetupReplication(source.DatabaseCommands, destination);

                sourceReplicationTask.IndexReplication.Execute(); //replicate the usual index

                source.SideBySideExecuteIndex(testIndex);

                //the side by side index will be automatically replicated
                SpinWait.SpinUntil(() =>
                {
                    var index = destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName);
                    return index != null;
                }, 5000);

                Assert.NotNull(destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName));

                destinationDatabase.Indexes.DeleteIndex(testIndex.IndexName); //delete the original index

                var sideBySideIndex = destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName);
                Assert.NotNull(sideBySideIndex);

                VerifyReplacementDocumentIsThere(Constants.SideBySideIndexNamePrefix + testIndex.IndexName, destinationDatabase);

                sourceReplicationTask.IndexReplication.Execute();

                destinationDatabase.SpinBackgroundWorkers();
                WaitForIndexing(destination);

                //wait until the index will be replaced
                SpinWait.SpinUntil(() =>
                {
                    var index = destinationDatabase.Indexes.GetIndexDefinition(testIndex.IndexName);
                    return index != null;
                }, 5000);

                var oldIndex = destinationDatabase.Indexes.GetIndexDefinition(testIndex.IndexName);
                Assert.True(oldIndex.Equals(testIndex.CreateIndexDefinition(), false));

                sideBySideIndex = destinationDatabase.Indexes.GetIndexDefinition(Constants.SideBySideIndexNamePrefix + testIndex.IndexName);
                Assert.Null(sideBySideIndex);

                SpinWait.SpinUntil(() =>
                {
                    var doc = destinationDatabase.Documents.Get(Constants.IndexReplacePrefix + Constants.SideBySideIndexNamePrefix + testIndex.IndexName, null);
                    return doc == null;
                }, 5000);

                Assert.Null(destinationDatabase.Documents.Get(Constants.IndexReplacePrefix + Constants.SideBySideIndexNamePrefix + testIndex.IndexName, null));
            }
        }

        private static void VerifyReplacementDocumentIsThere(string replacementIndexName, DocumentDatabase destinationDatabase,bool shouldVerifyEtag = false)
        {
            var id = Constants.IndexReplacePrefix + replacementIndexName;
            var documentInfo = destinationDatabase.Documents.Get(id, null);
            Assert.NotNull(documentInfo);

            if (shouldVerifyEtag)
            {
                var indexReplacementDoc = documentInfo.DataAsJson;
                Assert.Equal(destinationDatabase.Statistics.LastDocEtag, Etag.Parse(indexReplacementDoc.Value<string>("MinimumEtagBeforeReplace")));
            }
        }
    }
}
