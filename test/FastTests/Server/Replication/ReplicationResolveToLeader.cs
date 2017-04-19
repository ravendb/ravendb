using System;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationResolveToDatabase : ReplicationTestsBase
    {
        [Fact]
        public async Task ResovleToDatabase()
        {
            await CreateRaftClusterAndGetLeader(2);
            const string databaseName = "ResovleToDatabase";

            using (var store1 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[0].WebUrls[0]
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[1].WebUrls[0]
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

                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

                var documentDatabase = await GetDocumentDatabaseInstanceFor(store2);

                //this waits for the information to propagate in the cluster
                await UpdateConflictResolver(store1, 
                        resovlerDbId: documentDatabase.DbId.ToString());

                
                Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
                Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
            }
        }

        [Fact]
        public async Task ResovleToDatabaseComplex()
        {
            // store2 <--> store1 --> store3
            await CreateRaftClusterAndGetLeader(3);
            const string databaseName = "ResovleToDatabaseComplex";

            using (var store1 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[0].WebUrls[0]
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[1].WebUrls[0]
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[2].WebUrls[0]
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

              
                Assert.Equal(2, WaitUntilHasConflict(store1, "foo/bar").Results.Length);
                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
                Assert.Equal(3, WaitUntilHasConflict(store3, "foo/bar", 3).Results.Length);

                // store2 <--> store1 <--> store3*               
                var documentDatabase = await GetDocumentDatabaseInstanceFor(store3);
                //this waits for the information to propagate in the cluster
                await UpdateConflictResolver(store3,
                    resovlerDbId: documentDatabase.DbId.ToString());

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
                DefaultDatabase = databaseName,
                Url = Servers[0].WebUrls[0]
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[1].WebUrls[0]
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[2].WebUrls[0]
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
            await CreateRaftClusterAndGetLeader(2);
            const string databaseName = "UnsetDatabaseResolver";

            using (var store1 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[0].WebUrls[0]
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[1].WebUrls[0]
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

                // store1* --> store2
                var databaseResovlerId = GetDocumentDatabaseInstanceFor(store1).Result.DbId.ToString();
                await UpdateConflictResolver(store2, databaseResovlerId);
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
            await CreateRaftClusterAndGetLeader(2);
            const string databaseName = "UnsetDatabaseResolver";

            using (var store1 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[0].WebUrls[0]
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                DefaultDatabase = databaseName,
                Url = Servers[1].WebUrls[0]
            }.Initialize())
            {
                await CreateAndWaitForClusterDatabase(databaseName, store1);
                var documentDatabase1 = await GetDocumentDatabaseInstanceFor(store1);
                var documentDatabase2 = await GetDocumentDatabaseInstanceFor(store2);

                documentDatabase1.ReplicationLoader.OutgoingHandlers.FirstOrDefault().PauseReplication();
                documentDatabase2.ReplicationLoader.OutgoingHandlers.FirstOrDefault().PauseReplication();

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
                using (var session = store2.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                documentDatabase1.ReplicationLoader.OutgoingHandlers.FirstOrDefault().ResumeReplication();
                documentDatabase2.ReplicationLoader.OutgoingHandlers.FirstOrDefault().ResumeReplication();


                Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
                await UpdateConflictResolver(store1, documentDatabase2.DbId.ToString());

                Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
                Assert.Equal(1, WaitUntilHasTombstones(store1).Count);

            }
        }
    }
}
