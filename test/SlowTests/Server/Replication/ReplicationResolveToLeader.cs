using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationResolveToDatabase : ReplicationTestBase, IDocumentTombstoneAware
    {
        [Fact]
        public async Task ResolveToDatabase()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store1, store2);
               
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);

                var documentDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database).Result;

                await UpdateConflictResolver(store2, 
                        resovlerDbId: documentDatabase.DbBase64Id);
                await SetupReplicationAsync(store2, store1);
                
                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
            }
        }

        [Fact]
        public async Task ResolveToDatabaseComplex()
        {
            // store2 <--> store1 --> store3
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            using (var store3 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store3.OpenSession())
                {
                    session.Store(new User {Name = "Leader"}, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2, store3);
                await SetupReplicationAsync(store2, store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);
                Assert.Equal(3, WaitUntilHasConflict(store3, "foo/bar", 3).Length);

                // store2 <--> store1 <--> store3*               
                var documentDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store3.Database).Result;
                //this waits for the information to propagate in the cluster
                await UpdateConflictResolver(store3, resovlerDbId: documentDatabase.DbBase64Id);
                await SetupReplicationAsync(store3, store1);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Leader"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Leader"));
                Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Leader"));
            }
        }

        [Fact(Skip = "The scenario as it played here should and will resolve in error")]
        //not sure why to have such extremely complicated unit test...
        public async Task ChangeDatabaseAndResolve()
        {
            await CreateRaftClusterAndGetLeader(3);
            const string databaseName = "ChangeDatabaseAndResolve";

            using (var store1 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] {Servers[0].WebUrl}
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] {Servers[1].WebUrl}
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] {Servers[2].WebUrl}
            }.Initialize())
            {
                await CreateAndWaitForClusterDatabase(databaseName, store1);
                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store3.OpenSession())
                {
                    session.Store(new User {Name = "Leader"}, "foo/bar");
                    session.SaveChanges();
                }
                // store2 <-- store1 --> store3
                Assert.Equal(2, WaitUntilHasConflict(store3, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);

                // store2* <--> store1 --> store3
                var documentDatabase = await GetDocumentDatabaseInstanceFor(store2);
                //this waits for the information to propagate in the cluster
                await UpdateConflictResolver(store1,
                    resovlerDbId: documentDatabase.DbBase64Id);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Oren"));


                // store2 <--> store1 --> store3*
                var databaseResolverId = GetDocumentDatabaseInstanceFor(store3).Result.DbBase64Id;
                await UpdateConflictResolver(store3, databaseResolverId);

                using (var session = store3.OpenSession())
                {
                    session.Store(new User {Name = "Leader"}, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }

                // store2 <--> store1 --> store3*
                databaseResolverId = GetDocumentDatabaseInstanceFor(store3).Result.DbBase64Id;
                await UpdateConflictResolver(store3, databaseResolverId);

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Leader"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Leader"));
                Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Leader"));
            }
        }

        [Fact]
        public async Task UnsetDatabaseResolver()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }

                // store1* --> store2
                var documentDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;
                var databaseResolverId = documentDatabase.DbBase64Id;
                await UpdateConflictResolver(store2, databaseResolverId);
                await SetupReplicationAsync(store1, store2);

                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Karmel"));

                await UpdateConflictResolver(store2); // reset the conflict resovler
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "NewKarmel"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "NewOren"}, "foo/bar");
                    session.SaveChanges();
                }
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);
            }
        }       

        [Fact]
        public async Task ResolveToTombstone()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var documentDatabase1 = await GetDocumentDatabaseInstanceFor(store1);
                var documentDatabase2 = await GetDocumentDatabaseInstanceFor(store2);

                documentDatabase1.DocumentTombstoneCleaner.Subscribe(this);
                documentDatabase2.DocumentTombstoneCleaner.Subscribe(this);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User {Name = "Oren"}, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1,store2);
                await SetupReplicationAsync(store2,store1);

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Length);
                
                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await UpdateConflictResolver(store1, documentDatabase2.DbBase64Id);

                Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
                Assert.Equal(1, WaitUntilHasTombstones(store1).Count);

            }
        }

        public Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            return new Dictionary<string, long>
            {
                ["Users"] = 0
            };
        }
    }
}
