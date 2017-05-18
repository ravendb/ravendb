using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationResolveToDatabase : ReplicationTestsBase, IDocumentTombstoneAware
    {
        [Fact]
        public async Task ResovleToDatabase()
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
               
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                var documentDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database).Result;

                await UpdateConflictResolver(store2, 
                        resovlerDbId: documentDatabase.DbId.ToString());
                await SetupReplicationAsync(store2, store1);
                
                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
            }
        }

        [Fact]
        public async Task ResovleToDatabaseComplex()
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

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
                Assert.Equal(3, WaitUntilHasConflict(store3, "foo/bar", 3).Results.Length);

                // store2 <--> store1 <--> store3*               
                var documentDatabase = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store3.Database).Result;
                //this waits for the information to propagate in the cluster
                await UpdateConflictResolver(store3,
                    resovlerDbId: documentDatabase.DbId.ToString());
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
                Urls = Servers[0].WebUrls
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Database = databaseName,
                Urls = Servers[1].WebUrls
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Database = databaseName,
                Urls = Servers[2].WebUrls
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
                Assert.Equal(2, WaitUntilHasConflict(store3, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                // store2* <--> store1 --> store3
                var documentDatabase = await GetDocumentDatabaseInstanceFor(store2);
                //this waits for the information to propagate in the cluster
                await UpdateConflictResolver(store1,
                    resovlerDbId: documentDatabase.DbId.ToString());

                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Oren"));


                // store2 <--> store1 --> store3*
                var databaseResovlerId = GetDocumentDatabaseInstanceFor(store3).Result.DbId.ToString();
                await UpdateConflictResolver(store3, databaseResovlerId);

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
                databaseResovlerId = GetDocumentDatabaseInstanceFor(store3).Result.DbId.ToString();
                await UpdateConflictResolver(store3, databaseResovlerId);

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
                var databaseResovlerId = documentDatabase.DbId.ToString();
                await UpdateConflictResolver(store2, databaseResovlerId);
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
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
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

                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
                
                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await UpdateConflictResolver(store1, documentDatabase2.DbId.ToString());

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
