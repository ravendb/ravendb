using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Documents.Attachments;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19049 : RavenTestBase
    {
        public RavenDB_19049(ITestOutputHelper output) : base(output)
        {
        }

        private async Task<(string Id, string Name)> Setup(DocumentStore store)
        {
            var id = "users/1";
            var name = "file.txt";

            using (var session = store.OpenAsyncSession())
            using (var fileStream = new MemoryStream([1, 2, 3, 4, 5, 6, 7, 8, 9]))
            {
                var user = new User { Name = "Fitzchak" };
                await session.StoreAsync(user, id);

                session.Advanced.Attachments.Store(user, name, fileStream);

                await session.SaveChangesAsync();
            }

            return (id, name);
        }

        [Fact]
        public async Task ReturnsRange()
        {
            using var store = GetDocumentStore();
            var (id, name) = await Setup(store);

            using (var result = new MemoryStream())
            using (var session = store.OpenAsyncSession())
            {
                var attachment = await session.Advanced.Attachments.GetRangeAsync(id, name, 3, 5);

                await attachment.Stream.CopyToAsync(result);
                var bytes = result.ToArray();

                Assert.Equal(attachment.Details.Size, 9);
                Assert.Equal(bytes.Length, 3);
                Assert.Equal(bytes, [4, 5, 6]);
            }
        }

        [Fact]
        public async Task ThrowsForUnsupportedRange()
        {
            using var store = GetDocumentStore();
            var (id, name) = await Setup(store);

            using (var session = store.OpenAsyncSession())
            {
                await Assert.ThrowsAsync<InvalidAttachmentRangeException>(
                    () => session.Advanced.Attachments.GetRangeAsync(id, name, 999, 1000)
                );

                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                    () => session.Advanced.Attachments.GetRangeAsync(id, name, -5, 5)
                );

                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                    () => session.Advanced.Attachments.GetRangeAsync(id, name, 10, 5)
                );
            }
        }
    }
}
