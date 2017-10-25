using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Voron.Compaction;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents
{
    // TODO Add more tests for compacting documents + indexes, documents alone and indexes alone
    public class CompactDatabaseTest : RavenTestBase
    {
        [Fact]
        public async Task CanCompactDatabase()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                store.Admin.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    await store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                    })).WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                }

                WaitForIndexing(store);

                var deleteOperation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));


                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var compactOperation = store.Operations.Send(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" }
                }), isServerOperation: true);
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }
        }

        [Fact]
        public async Task CanCompactDatabaseWithAttachment()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
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

                var operation = await store.Operations.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }), isServerOperation: true);
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }

        }
    }
}
