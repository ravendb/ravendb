using System.Diagnostics;
using System.Linq;
using System.Threading;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Replication;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationResolveToDatabase : ReplicationTestsBase
    {
        [Fact]
        public void ResovleToDatabase()
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

            Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

            SetupReplication(store2, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store2).Result.DbId.ToString(),
                    Version = 0
                }
            }, store1);

            Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
            Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
        }

        [Fact]
        public void ResovleToDatabaseComplex()
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

            Assert.Equal(2,WaitUntilHasConflict(store1,"foo/bar").Results.Length);
            Assert.Equal(2,WaitUntilHasConflict(store2,"foo/bar").Results.Length);
            Assert.Equal(3,WaitUntilHasConflict(store3,"foo/bar", 3).Results.Length);

            // store2 <--> store1 <--> store3*
            SetupReplication(store3, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store3).Result.DbId.ToString(),
                    Version = 0
                }
            },store1);


            Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Leader"));
            Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Leader"));
            Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Leader"));
        }

        [Fact]
        public void ChangeDatabaseAndResolve()
        {           
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
            // store2 <-- store1 --> store3
            SetupReplication(store1, store2, store3);
           
            Assert.Equal(2, WaitUntilHasConflict(store3, "foo/bar").Results.Length);
            Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

            // store2* <--> store1 --> store3
            
            SetupReplication(store2, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store2).Result.DbId.ToString(),
                    Version = 0
                }
            }, store1);


            Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Oren"));
            Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Oren"));
            Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Oren"));


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

            Assert.True(WaitForDocument<User>(store1, "foo/bar", u => u.Name == "Leader"));
            Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Leader"));
            Assert.True(WaitForDocument<User>(store3, "foo/bar", u => u.Name == "Leader"));
        }

        [Fact]
        public void UnsetDatabaseResolver()
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

            Assert.True(WaitForDocument<User>(store2, "foo/bar", u => u.Name == "Karmel"));

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
            Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);
        }

        [Fact]
        public void SetDatabaseResolverAtTwoNodes()
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


            var mre = new ManualResetEventSlim();

            var database2 = GetDocumentDatabaseInstanceFor(store2).Result;
            database2.ReplicationLoader.ReplicationFailed += (src, ex) =>
            {
                mre.Set();
            };

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
                    ResolvingDatabaseId = database2.DbId.ToString(),
                    Version = 0
                }
            }, store1);

            var millisecondsTimeout = 1500;
            if (Debugger.IsAttached)
                millisecondsTimeout *= 100;
            Assert.True(mre.Wait(millisecondsTimeout));

            var failures = GetConnectionFaliures(store1);
            Assert.True(failures[store2.DefaultDatabase].Any(
                v=>v.Contains("Resolver versions are conflicted. Same version 0, but different")));
           
        }

        [Fact]
        public void ResolveToTombstone()
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
            using (var session = store2.OpenSession())
            {
                session.Delete("foo/bar");
                session.SaveChanges();
            }
            SetupReplication(store1, store2);

            Assert.Equal(2, WaitUntilHasConflict(store2, "foo/bar").Results.Length);

            SetupReplication(store2, new ReplicationDocument
            {
                DefaultResolver = new DatabaseResolver
                {
                    ResolvingDatabaseId = GetDocumentDatabaseInstanceFor(store2).Result.DbId.ToString(),
                    Version = 0
                }
            }, store1);
            
            Assert.Equal(1,WaitUntilHasTombstones(store1).Count);
            Assert.Equal(1,WaitUntilHasTombstones(store2).Count);
        }
    }
}
