using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class OptimisticConcurrency : ReplicationTestBase
    {
        public OptimisticConcurrency(ITestOutputHelper output) : base(output)
        {
        }

        public class Foo
        {
            public string Name; 
        }
      
        [Fact]
        public void store_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled()
        {
            using (var store = GetDocumentStore())
            {
                string fooId = "Foos/1";

                using (var session = store.OpenSession())
                {
                    var foo = new Foo { Name = "One" };
                    session.Store(foo, fooId);
                    session.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    newSession.Advanced.UseOptimisticConcurrency = true;
                    var foo = new Foo { Name = "Two" };
                    newSession.Store(foo, fooId);
                    var e = Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }

        [Theory]
        [InlineData("")]
        [InlineData("A:1-dummyDbId")]
        public void store_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled_with_invalid_change_vector(string changeVector)
        {
            using (var store = GetDocumentStore())
            {
                string fooId = "Foos/1";

                using (var session = store.OpenSession())
                {
                    var foo = new Foo { Name = "One" };
                    session.Store(foo, fooId);
                    session.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    newSession.Advanced.UseOptimisticConcurrency = true;
                    var foo = new Foo { Name = "Two" };
                    newSession.Store(foo, changeVector: changeVector, fooId);
                    var e = Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }

        [Fact]
        public void delete_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled()
        {
            using (var store = GetDocumentStore())
            {
                string fooId = "Foos/1";

                using (var session = store.OpenSession())
                {
                    var foo = new Foo { Name = "One" };
                    session.Store(foo, fooId);
                    session.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    newSession.Advanced.UseOptimisticConcurrency = true;
                    newSession.Delete(fooId, "A:1-dummy");
                    var e = Assert.Throws<ConcurrencyException>(() =>
                    {
                        newSession.SaveChanges();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }

        [Fact]
        public async Task delete_should_throw_exception_if_doc_exists_and_optimistic_concurrency_is_enabled_async()
        {
            using (var store = GetDocumentStore())
            {
                const string fooId = "Foos/1";

                using (var session = store.OpenAsyncSession())
                {
                    var foo = new Foo { Name = "One" };
                    await session.StoreAsync(foo, fooId);
                    await session.SaveChangesAsync();
                }

                using (var newSession = store.OpenAsyncSession())
                {
                    newSession.Advanced.UseOptimisticConcurrency = true;
                    newSession.Delete(fooId, "A:1-dummy");
                    var e = await Assert.ThrowsAsync<ConcurrencyException>(async () =>
                    {
                        await newSession.SaveChangesAsync();
                    });
                    Assert.StartsWith("Document Foos/1 has change vector A:1-", e.Message);
                }
            }
        }


        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

        [Fact]
        public async Task MergedBatchCommandPut_WhenDocumentAlreadyDeletedAndFailOnConcurrency_ShouldNotDeleteTombstone()
        {
            var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.UseOptimisticConcurrency = true,
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            });

            const string id = "TestObjs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();

                session.Delete(id);
                await session.SaveChangesAsync();
            }

            Assert.NotEqual(0, GetTombstones(store).Count);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), "someConflictedChangeVector", id);
                await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await session.SaveChangesAsync());
            }
            Assert.NotEqual(0, GetTombstones(store).Count);
        }

        [Fact]
        public async Task MergedBatchCommandPatch_WhenDocumentAlreadyDeletedAndFailOnConcurrency_ShouldNotDeleteTombstone()
        {
            var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.UseOptimisticConcurrency = true,
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            });

            const string id = "TestObjs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();

                session.Delete(id);
                await session.SaveChangesAsync();
            }

            Assert.NotEqual(0, GetTombstones(store).Count);
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Defer(new PatchCommandData(id: id, changeVector: "someConflictedChangeVector", patch: new PatchRequest { Script = @"this.Prop = Changed" }, null));
                await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await session.SaveChangesAsync());
            }
            Assert.NotEqual(0, GetTombstones(store).Count);
        }

        [Fact]
        public async Task MergedPutCommand_WhenDocumentAlreadyDeletedAndFailOnConcurrency_ShouldNotDeleteTombstone()
        {
            var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.UseOptimisticConcurrency = true,
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            });

            const string id = "TestObjs/1";
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new TestObj(), id);
                await session.SaveChangesAsync();
            }

            using (var session1 = store.OpenAsyncSession())
            {
                session1.Delete(id);
                await session1.SaveChangesAsync().ContinueWith(_ => "");
            }
            Assert.NotEqual(0, GetTombstones(store).Count);

            var requestExecuter = store.GetRequestExecutor();
            using (requestExecuter.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var djv = new DynamicJsonValue
                {
                    [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Raven.Client.Constants.Documents.Metadata.Collection] = store.Conventions.FindCollectionName(typeof(TestObj))
                    }
                };
                var json = context.ReadObject(djv, id);
                var putCommand = new PutDocumentCommand(id, "someConflictedChangeVector", json);
                await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await requestExecuter.ExecuteAsync(putCommand, context));
            }

            Assert.NotEqual(0, GetTombstones(store).Count);
        }

        [Fact]
        public async Task MergedPutAttachmentCommand_WhenAttachmentAlreadyDeletedAndFailOnConcurrency_ShouldNotDeleteTombstone()
        {
            var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.UseOptimisticConcurrency = true,
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            });

            const string id = "TestObjs/1";
            const string attachmentName = "attachmentName";

            using (var session = store.OpenAsyncSession())
            {
                var testObj = new TestObj();
                await session.StoreAsync(testObj, id);
                session.Advanced.Attachments.Store(testObj, attachmentName, new MemoryStream(new byte[] { 1 }));
                await session.SaveChangesAsync();

                session.Advanced.Attachments.Delete(id, attachmentName);
                await session.SaveChangesAsync();
            }

            Assert.NotEqual(0, GetTombstones(store).Count);
            var operation = new PutAttachmentOperation(id, attachmentName, new MemoryStream(new byte[] { 1 }), changeVector: "someConflictedChangeVector");
            await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await store.Operations.SendAsync(operation));
            Assert.NotEqual(0, GetTombstones(store).Count);
        }

        [Fact]
        public async Task MergedDeleteAttachmentCommand_WhenAttachmentAlreadyDeletedAndFailOnConcurrency_ShouldNotDeleteTombstone()
        {
            var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.UseOptimisticConcurrency = true,
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            });

            const string id = "TestObjs/1";
            const string attachmentName = "attachmentName";

            using (var session = store.OpenAsyncSession())
            {
                var testObj = new TestObj();
                await session.StoreAsync(testObj, id);
                session.Advanced.Attachments.Store(testObj, attachmentName, new MemoryStream(new byte[] { 1 }));
                await session.SaveChangesAsync();

                session.Advanced.Attachments.Delete(id, attachmentName);
                await session.SaveChangesAsync();
            }

            Assert.NotEqual(0, GetTombstones(store).Count);
            var operation = new DeleteAttachmentOperation(id, attachmentName, changeVector: "someConflictedChangeVector");
            await Assert.ThrowsAnyAsync<ConcurrencyException>(async () => await store.Operations.SendAsync(operation));
            Assert.NotEqual(0, GetTombstones(store).Count);
        }
    }
}
