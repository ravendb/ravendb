using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22932 : RavenTestBase
    {
        public RavenDB_22932(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Sharding)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanCompactDatabaseOrShardWithData(Options options, bool withAttachment)
        {
            options.RunInMemory = false; // compaction requires on-disk storage

            using (var store = GetDocumentStore(options))
            {
                const string id = "users/1";
                var content = new byte[] { 1, 2, 3, 4 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, id);

                    if (withAttachment)
                        session.Advanced.Attachments.Store(id, "file.bin", new MemoryStream(content), "application/octet-stream");

                    await session.SaveChangesAsync();
                }

                // a sharded database must be compacted shard by shard, so compact the shard that holds the document
                var databaseName = options.DatabaseMode == RavenDatabaseMode.Sharded
                    ? ShardHelper.ToShardName(store.Database, await Sharding.GetShardNumberForAsync(store, id))
                    : store.Database;

                var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = databaseName,
                    Documents = true
                }));

                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    if (withAttachment)
                    {
                        using (var attachment = await session.Advanced.Attachments.GetAsync(id, "file.bin"))
                        using (var stream = new MemoryStream())
                        {
                            Assert.NotNull(attachment);
                            await attachment.Stream.CopyToAsync(stream);
                            Assert.Equal(content, stream.ToArray());
                        }
                    }
                }

                if (options.DatabaseMode != RavenDatabaseMode.Sharded)
                    return;

                // loading by id does not read the bucket index - the bucket is recomputed on the orchestrator and the
                // shard then reads by primary key - so a wrong bucket written during compaction would go unnoticed.
                // these read through the index the compaction rebuilt.
                var bucket = await Sharding.GetBucketAsync(store, id);
                var shard = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(databaseName);

                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var documents = shard.ShardedDocumentsStorage.GetDocumentsByBucketFrom(ctx, bucket, etag: 0).ToList();
                    Assert.Equal(id, Assert.Single(documents).Id);

                    if (withAttachment)
                    {
                        var attachments = shard.DocumentsStorage.AttachmentsStorage
                            .GetAttachmentsByBucketFrom(ctx, bucket, etag: 0).ToList();

                        Assert.Equal("file.bin", Assert.Single(attachments).Name);
                    }
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
