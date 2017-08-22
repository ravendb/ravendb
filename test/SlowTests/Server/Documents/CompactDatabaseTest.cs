using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Voron.Compaction;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents
{
    public class CompactDatabaseTest : RavenTestBase
    {
        [Fact(Skip = "RavenDB-8161")]
        public async Task CanCompactDatabase()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                store.Admin.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    await store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = "FROM Orders"
                    }, new PatchRequest()
                    {
                        Script = @"put(""orders/"", this);"
                    })).WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                }

                WaitForIndexing(store);

                var deleteOperation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));


                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var requestExecutor = store.GetRequestExecutor();
                long compactOperationId;
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var getOperationIdCommand = new GetNextOperationIdCommand();
                    await requestExecutor.ExecuteAsync(getOperationIdCommand, context);
                    compactOperationId = getOperationIdCommand.Result;
                }

                var compactOperation = store.Operations.Send(new CompactDatabaseOperation(store.Database, compactOperationId));
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize < newSize);
            }
        }

        [Fact]
        public async Task CanCompactDatabaseWithAttachment()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                var buffer = new byte[16 * 1024 * 1024];
                new Random().NextBytes(buffer);

                using (var session = store.OpenSession())
                using (var fileStream = new MemoryStream(buffer))
                {
                    var user = new User
                    {
                        Name = "Iftah"
                    };
                    session.Store(user, "users/1");
                    session.Advanced.StoreAttachment(user, "randomFile.txt", fileStream);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.DeleteAttachment("users/1", "randomFile.txt");
                    session.SaveChanges();
                }

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var requestExecutor = store.GetRequestExecutor();
                long compactOperationId;
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var getOperationIdCommand = new GetNextOperationIdCommand();
                    await requestExecutor.ExecuteAsync(getOperationIdCommand, context);
                    compactOperationId = getOperationIdCommand.Result;
                }

                var operation = await store.Operations.SendAsync(new CompactDatabaseOperation(store.Database, compactOperationId), isServerOperation: true);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }

        }
    }
}
