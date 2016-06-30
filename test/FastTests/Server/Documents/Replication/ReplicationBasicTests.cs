using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Documents.Replication
{
    public class ReplicationBasicTests : ReplicationTestsBase
    {
        public readonly string DbName = "TestDB" + Guid.NewGuid();

        public class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact(Skip = "WIP, should not pass yet")]
        public async Task Master_master_replication_from_etag_zero_without_conflict_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
            using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
            {
                store1.DefaultDatabase = dbName1;
                store2.DefaultDatabase = dbName2;

                SetupReplication(dbName2, store1, store2);
                SetupReplication(dbName1, store2, store1);
                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");
                    session.SaveChanges();
                }
                using (var session = store2.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store1, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store1, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);

                replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 10000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 10000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }		

        [Fact(Skip = "WIP, should not pass yet")]
        public async Task TryGetEmbedded_should_work_properly()
        {
            string dbName = $"TestDB{Guid.NewGuid()}";
            using (await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName))
            using (var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName))
            {
                DocumentsOperationContext context;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var doc = context.ReadObject(new DynamicJsonValue
                    {
                        ["Foo"] = "A",
                        ["Inner"] = new DynamicJsonValue
                        {
                            ["Bar"] = "ABC"
                        }
                    }, "foo/bar");

                    using (context.OpenWriteTransaction())
                    {
                        BlittableJsonReaderObject inner;
                        doc.TryGet("Inner", out inner);
                        Assert.NotNull(inner);
                        db.DocumentsStorage.Put(context, "foo/bar", null, inner);
                        context.Transaction.Commit();
                    }

                    using (context.OpenWriteTransaction())
                    {
                        BlittableJsonReaderObject inner;
                        doc.TryGet("Inner", out inner);
                        inner.Modifications = new DynamicJsonValue(inner)
                        {
                            ["Bar"] = "AAA"
                        };
                        db.DocumentsStorage.Put(context, "foo/bar", null, inner);
                        context.Transaction.Commit();
                    }

                    using (context.OpenReadTransaction())
                    {
                        //should fetch only embedded object and not its parent
                        var fetchedDoc =  db.DocumentsStorage.Get(context, "foo/bar");
                        object _;
                        Assert.False(fetchedDoc.Data.TryGet("Inner",out _));
                        Assert.True(fetchedDoc.Data.TryGet("Bar", out _));
                    }
                }
            }
        }

        [Fact(Skip = "WIP, should not pass yet")]
        public async Task Master_slave_replication_from_etag_zero_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
            using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
            {
                store1.DefaultDatabase = dbName1;
                store2.DefaultDatabase = dbName2;

                SetupReplication(dbName2, store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    },"users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    },"users/2");

                    session.SaveChanges();		
                }

                var replicated1 = WaitForDocumentToReplicate<User>(store2, "users/1", 15000);

                Assert.NotNull(replicated1);
                Assert.Equal("John Dow", replicated1.Name);
                Assert.Equal(30, replicated1.Age);

                var replicated2 = WaitForDocumentToReplicate<User>(store2, "users/2", 5000);
                Assert.NotNull(replicated2);
                Assert.Equal("Jane Dow", replicated2.Name);
                Assert.Equal(31, replicated2.Age);
            }
        }

        [Fact(Skip = "WIP, should not pass yet")]
        public async Task Master_slave_replication_with_multiple_PUTS_should_work()
        {
            var dbName1 = DbName + "-1";
            var dbName2 = DbName + "-2";
            using (var store1 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName1))
            using (var store2 = await GetDocumentStore(modifyDatabaseDocument: document => document.Id = dbName2))
            {
                store1.DefaultDatabase = dbName1;
                store2.DefaultDatabase = dbName2;

                SetupReplication(dbName2, store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Dow",
                        Age = 30
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Jane Dow",
                        Age = 31
                    }, "users/2");

                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Jack Dow",
                        Age = 33
                    }, "users/3");

                    session.Store(new User
                    {
                        Name = "Jessy Dow",
                        Age = 34
                    }, "users/4");

                    session.SaveChanges();
                }

                await store2.Replication.WaitAsync();
                using (var session = store2.OpenSession())
                {
                    var docs = session.Load<User>(new[]
                    {
                        "users/1",
                        "users/2",
                        "users/3",
                        "users/4"
                    });

                    Assert.DoesNotContain(docs, d => d == null);
                    Assert.Contains(docs, d => d.Name.Equals("John Dow"));
                    Assert.Contains(docs, d => d.Name.Equals("Jane Dow"));
                    Assert.Contains(docs, d => d.Name.Equals("Jack Dow"));
                    Assert.Contains(docs, d => d.Name.Equals("Jessy Dow"));
                }

            }
        }
    }
}
