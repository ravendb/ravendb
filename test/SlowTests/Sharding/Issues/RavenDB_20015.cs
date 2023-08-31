using System.IO;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20015 : ReplicationTestBase
    {
        public RavenDB_20015(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Attachments | RavenTestCategory.Replication)]
        public async Task ShouldNotThrowNREWhenSendingAttachmentWithoutStream()
        {
            using (var source = GetDocumentStore())
            using (var destination = Sharding.GetDocumentStore())
            {
                await SetupReplicationAsync(source, destination);

                int i = 0;
                while (await Sharding.AllShardHaveDocsAsync(Server, destination.Database) == false)
                {
                    using (var session = source.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Foo" }, $"users/{i++}");
                        await session.SaveChangesAsync();
                    }
                }

                var docs = await Sharding.GetOneDocIdForEachShardAsync(Server, destination.Database);

                var b = await BreakReplication(Server.ServerStore, source.Database);

                using (var session = source.OpenAsyncSession())
                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    for (i = 0; i < docs.Count - 1; i++)
                    {
                        var docId = docs[i];

                        stream.Seek(0, SeekOrigin.Begin);
                        session.Advanced.Attachments.Store(docId, "foo.png", stream, "image/png");
                        await session.SaveChangesAsync();
                    }
                }

                b.Mend();

                using (var session = source.OpenAsyncSession())
                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var stream2 = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                {
                    var docId = docs[0];
                    var docId2 = docs[i];

                    session.Advanced.Attachments.Store(docId, "foo2.png", stream2, "image/png");
                    session.Advanced.Attachments.Store(docId2, "foo.png", stream, "image/png");
                    await session.SaveChangesAsync();
                }

                foreach (var kvp in docs)
                {
                    var docId = kvp.Value;
                    Assert.NotNull(WaitForDocumentWithAttachmentToReplicate<User>(destination, docId, "foo.png", 30 * 1000));
                }

                for (i = 0; i < docs.Count; i++)
                {
                    var docId = docs[i];
                    using (var session = destination.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(docId);
                        var attachments = session.Advanced.Attachments.GetNames(user);
                        var expected = i == 0 ? 2 : 1;
                        Assert.Equal(expected, attachments.Length);
                    }
                }
            }
        }
    }
}
