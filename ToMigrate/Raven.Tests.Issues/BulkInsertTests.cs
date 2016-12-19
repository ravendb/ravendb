using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class BulkInsertTests : RavenTest
    {
        [Fact]
        public void CanCreateAndDisposeUsingBulk2()
        {
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new UserNoId { Name = "Fitzchak" }, "users/1");
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserNoId>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Fitzchak", user.Name);
                }
            }
        }

        private class UserNoId
        {
            public string Name { get; set; }
        }
        [Fact]
        public void CanCreateAndDisposeUsingBulk()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var bulkInsertOperation = new RemoteBulkInsertOperation(new BulkInsertOptions(), (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes());
                bulkInsertOperation.Dispose();
            }
        }

        [Fact]
        public void CanHandleUpdates()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var op = new RemoteBulkInsertOperation(new BulkInsertOptions(), (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes()))
                {
                    op.Write("items/1", new RavenJObject(), new RavenJObject());
                }

                using (var op = new RemoteBulkInsertOperation(new BulkInsertOptions
                {
                    OverwriteExisting = true
                }, (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes()))
                {
                    op.Write("items/1", new RavenJObject(), new RavenJObject());
                }
            }
        }


        [Fact]
        public void CanHandleReferenceChecking()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var op = new RemoteBulkInsertOperation(new BulkInsertOptions
                {
                    CheckReferencesInIndexes = true
                }, (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes()))
                {
                    op.Write("items/1", new RavenJObject(), new RavenJObject());
                }
            }
        }

        [Fact]
        public void CanInsertSingleDocument()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var bulkInsertOperation = new RemoteBulkInsertOperation(new BulkInsertOptions(),
                                                                        (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes());
                bulkInsertOperation.Write("test", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Dispose();

                Assert.Equal("passed", store.DatabaseCommands.Get("test").DataAsJson.Value<string>("test"));
            }
        }

        [Fact]
        public void StoreWithSpacesInDocumentId()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new UserNoId { Name = "Idan" }, "users/12     ");
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<UserNoId>("users/12     ");
                    Assert.NotNull(user);
                    Assert.Equal("Idan", user.Name);
                }
            }
        }

        private class UserWithId
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
        [Fact]
        public void StoreIdWithSlash()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new UserWithId { Name = "IdanWithSlash" }, "users/");
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<UserWithId>().Where(x=>x.Name.Equals("IdanWithSlash"));
                    foreach (var user in users)
                    {
                        Assert.False(user.Id[user.Id.Length-1] == '/');   
                    }
                }
            }
        }

        [Fact]
        public void CanInsertSeveralDocuments()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var bulkInsertOperation = new RemoteBulkInsertOperation(new BulkInsertOptions(),
                                                                        (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes());
                bulkInsertOperation.Write("one", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Write("two", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Dispose();

                Assert.Equal("passed", store.DatabaseCommands.Get("one").DataAsJson.Value<string>("test"));
                Assert.Equal("passed", store.DatabaseCommands.Get("two").DataAsJson.Value<string>("test"));
            }
        }

        [Fact]
        public void CanInsertSeveralDocumentsInSeveralBatches()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var bulkInsertOperation = new RemoteBulkInsertOperation(new BulkInsertOptions { BatchSize = 2 },
                                                                        (AsyncServerClient)store.AsyncDatabaseCommands, store.Changes());
                bulkInsertOperation.Write("one", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Write("two", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Write("three", new RavenJObject(), new RavenJObject { { "test", "passed" } });
                bulkInsertOperation.Dispose();

                Assert.Equal("passed", store.DatabaseCommands.Get("one").DataAsJson.Value<string>("test"));
                Assert.Equal("passed", store.DatabaseCommands.Get("two").DataAsJson.Value<string>("test"));
                Assert.Equal("passed", store.DatabaseCommands.Get("three").DataAsJson.Value<string>("test"));
            }
        }
    }
}
