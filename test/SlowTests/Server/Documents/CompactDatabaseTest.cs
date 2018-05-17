using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Voron.Compaction;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents
{
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
                store.Maintenance.Send(new CreateSampleDataOperation());

                for (int i = 0; i < 3; i++)
                {
                    await store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"FROM Orders UPDATE { put(""orders/"", this); } "
                    })).WaitForCompletionAsync(TimeSpan.FromSeconds(300));
                }

                WaitForIndexing(store);

                var deleteOperation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM orders" }));
                await deleteOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));


                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var compactOperation = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true,
                    Indexes = new[] { "Orders/ByCompany", "Orders/Totals" }
                }));
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
                    session.Advanced.Attachments.Store(user, "randomFile.txt", fileStream);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "randomFile.txt");
                    session.SaveChanges();
                }

                var oldSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }));
                await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                var newSize = StorageCompactionTestsSlow.GetDirSize(new DirectoryInfo(path));

                Assert.True(oldSize > newSize);
            }

        }
    }
}
