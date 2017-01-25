using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Replication;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationResolveToDatabase : ReplicationTestsBase
    {
        [Fact]
        public async Task ResovleToDatabase()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
            using (var session = store1.OpenSession())
            {
                session.Store(new User{Name = "Karmel"},"foo/bar");
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }
         
            SetupReplication(store1,store2);

            var conflicts = WaitUntilHasConflict(store2, "foo/bar");
            Assert.Equal(2, conflicts["foo/bar"].Count);

            SetupReplication(store2, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store2).Result.DbId.ToString(),
                    Version = 0
                }
            }, store1);

            await Task.Delay(500);

            var doc1 = WaitForDocument<User>(store1, "foo/bar");
            var doc2 = WaitForDocument<User>(store2, "foo/bar");
            Assert.Equal("Oren",doc1.Name);
            Assert.Equal("Oren",doc2.Name);
        }

        [Fact]
        public async Task ResovleToDatabaseComplex()
        {
            // store2 <--> store1 --> store3
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
            var store3 = GetDocumentStore();
            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store3.OpenSession())
            {
                session.Store(new User { Name = "Leader" }, "foo/bar");
                session.SaveChanges();
            }

            SetupReplication(store1, store2, store3);
            SetupReplication(store2, store1);

            await Task.Delay(500);

            Assert.Equal(2,WaitUntilHasConflict(store1,"foo/bar")["foo/bar"].Count);
            Assert.Equal(2,WaitUntilHasConflict(store2,"foo/bar")["foo/bar"].Count);
            Assert.Equal(3,WaitUntilHasConflict(store3,"foo/bar")["foo/bar"].Count);

            // store2 <--> store1 <--> store3*
            SetupReplication(store3, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store3).Result.DbId.ToString(),
                    Version = 0
                }
            },store1);

            var doc1 = WaitForDocument<User>(store1, "foo/bar");
            var doc2 = WaitForDocument<User>(store2, "foo/bar");
            var doc3 = WaitForDocument<User>(store3, "foo/bar");

            Assert.Equal("Leader", doc1.Name);
            Assert.Equal("Leader", doc2.Name);
            Assert.Equal("Leader", doc3.Name);
        }

        [Fact]
        public async Task ChangeDatabaseAndResolve()
        {           
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
            var store3 = GetDocumentStore();
            int delay = 500; 

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store3.OpenSession())
            {
                session.Store(new User { Name = "Leader" }, "foo/bar");
                session.SaveChanges();
            }
            // store2 <-- store1 --> store3
            SetupReplication(store1, store2, store3);
           
            Assert.Equal(2, WaitUntilHasConflict(store3, "foo/bar")["foo/bar"].Count);
            Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar")["foo/bar"].Count);

            // store2* <--> store1 --> store3
            
            SetupReplication(store2, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store2).Result.DbId.ToString(),
                    Version = 0
                }
            }, store1);

            await Task.Delay(delay);

            var doc2 = WaitForDocument<User>(store2, "foo/bar");
            Assert.Equal("Oren", doc2.Name);
            var doc1 = WaitForDocument<User>(store1, "foo/bar");
            Assert.Equal("Oren", doc1.Name);
            var doc3 = WaitForDocument<User>(store3, "foo/bar");
            Assert.Equal("Oren", doc3.Name);

            // store2 <--> store1 --> store3*
            SetupReplication(store3, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store3).Result.DbId.ToString(),
                    Version = 1
                }
            });

            using (var session = store3.OpenSession())
            {
                session.Store(new User { Name = "Leader" }, "foo/bar");
                session.SaveChanges();
            }

            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }

            // store2 <--> store1 --> store3*
            SetupReplication(store3, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store3).Result.DbId.ToString(),
                    Version = 2
                }
            },store1);

            await Task.Delay(delay);

            doc3 = WaitForDocument<User>(store3, "foo/bar");
            doc1 = WaitForDocument<User>(store1, "foo/bar");
            doc2 = WaitForDocument<User>(store2, "foo/bar");

            Assert.Equal("Leader", doc1.Name);
            Assert.Equal("Leader", doc2.Name);
            Assert.Equal("Leader", doc3.Name);
        }

        [Fact]
        public async Task UnsetDatabaseResolver()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }
            // store1* --> store2
            SetupReplication(store1, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store1).Result.DbId.ToString(),
                    Version = 0
                }
            }, store2);

            await Task.Delay(500);

            var doc1 = WaitForDocument<User>(store2, "foo/bar");
            Assert.Equal("Karmel", doc1.Name);

            SetupReplication(store1, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = null,
                    Version = 1
                }
            }, store2);

            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "NewKarmel" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "NewOren" }, "foo/bar");
                session.SaveChanges();
            }

            Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar")["foo/bar"].Count);
        }

        [Fact]
        public async Task SetDatabaseResolverAtTwoNodes()
        {
            var store1 = GetDocumentStore();
            var store2 = GetDocumentStore();
          
            using (var session = store1.OpenSession())
            {
                session.Store(new User { Name = "Karmel" }, "foo/bar");
                session.SaveChanges();
            }
            using (var session = store2.OpenSession())
            {
                session.Store(new User { Name = "Oren" }, "foo/bar");
                session.SaveChanges();
            }

            SetupReplication(store1, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store1).Result.DbId.ToString(),
                    Version = 0
                }
            });
 
            // store2* --> store1*
            SetupReplication(store2, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store2).Result.DbId.ToString(),
                    Version = 0
                }
            }, store1);

            await Task.Delay(500);

            var failures = GetConnectionFaliures(store1);
            Assert.True(failures[store2.DefaultDatabase].Any(
                v=>v.Contains("Resolver versions are conflicted. Same version 0, but different")));
           
        }
    }
}
