using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26789 : ClusterTestBase
    {
        public RavenDB_26789(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Attachments)]
        public async Task PuttingAttachmentOverMigratedTombstoneShouldPreserveTombstoneChangeVector()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string id = "users/1";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "1" }, id);
                    await session.SaveChangesAsync();
                }

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, "a1", stream, "a1/png"));
                }

                await store.Operations.SendAsync(new DeleteAttachmentOperation(id, "a1"));

                var bucket = await Sharding.GetBucketAsync(store, id);

                // bucket migration rewrites the attachment tombstone change vector into the composite 'order|version' shape
                await Sharding.Resharding.MoveShardForId(store, id);

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                var newShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                var storage = (ShardedDocumentsStorage)newShard.DocumentsStorage;

                string tombstoneChangeVector = null;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var tombstone in storage.RetrieveTombstonesByBucketFrom(context, bucket, 0))
                    {
                        if (tombstone.Type == Tombstone.TombstoneType.Attachment)
                            tombstoneChangeVector = tombstone.ChangeVector;
                    }
                }

                // preconditions: the tombstone was migrated and carries a composite change vector
                Assert.NotNull(tombstoneChangeVector);
                Assert.Contains("|", tombstoneChangeVector);

                // recreating the attachment over the migrated tombstone must supersede the tombstone
                AttachmentDetails putResult;
                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    putResult = await store.Operations.SendAsync(new PutAttachmentOperation(id, "a1", stream, "a1/png"));
                }

                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var status = ChangeVector.GetConflictStatus(context, tombstoneChangeVector, putResult.ChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                }
            }
        }
    }
}
