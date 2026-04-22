using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Config;
using Sparrow;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24450(ITestOutputHelper output) : ReplicationTestBase(output)
{
    [RavenTheory(RavenTestCategory.Attachments | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldAccountForAttachmentStreamSizeInReplicationBatchSize(Options options)
    {
        const int attachmentSize = 512 * 1024; // 512KB each
        const int documentCount = 10; // 5MB total in attachment streams
        const int maxSizeToSendMb = 1; // 1MB per batch

        const long maxSizeToSendBytes = maxSizeToSendMb * 1024L * 1024L;

        using var store1 = GetDocumentStore(new Options(options)
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Replication.MaxSizeToSend)] = maxSizeToSendMb.ToString();
                options.ModifyDatabaseRecord?.Invoke(record);
            }
        });

        using var store2 = GetDocumentStore(options);

        for (int i = 0; i < documentCount; i++)
        {
            byte[] data = GetAttachmentData(attachmentSize, i);

            using var session = store1.OpenAsyncSession();
            var docId = GetDocId(i);
            await session.StoreAsync(new { }, docId);
            session.Advanced.Attachments.Store(docId, GetAttachmentId(i), new MemoryStream(data));
            await session.SaveChangesAsync();
        }

        var sourceDb = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, GetDocId(0));

        await SetupReplicationAsync(store1, store2);
        await EnsureReplicatingAsync(store1, store2);

        // Verify all documents and attachments arrived
        using (var session = store2.OpenAsyncSession())
        {
            for (int i = 0; i < documentCount; i++)
            {
                var docId = GetDocId(i);
                var doc = await session.LoadAsync<object>(docId);
                Assert.NotNull(doc);

                using var attachment = await session.Advanced.Attachments.GetAsync(docId, GetAttachmentId(i));
                Assert.NotNull(attachment);
                Assert.Equal(attachmentSize, attachment.Details.Size);
            }
        }

        // Use the existing outgoing replication performance stats to verify batch sizes.
        // BatchSizeInBytes is accumulated from RecordAttachmentOutput, RecordAttachmentStreamOutput,
        // RecordDocumentOutput, etc. — it reflects the real bytes written per batch.
        var outgoingHandler = sourceDb.ReplicationLoader.OutgoingHandlers
            .First(h => h.Destination.Database == store2.Database || h.Destination.Database.StartsWith(store2.Database));
        var perfStats = outgoingHandler.GetReplicationPerformance();
        var completedBatches = perfStats.Where(s => s.BatchSizeInBytes.HasValue).ToList();

        Assert.True(completedBatches.Count > 1,
            $"Expected multiple replication batches due to attachment stream sizes, but got {completedBatches.Count}");

        // The batch size should respect MaxSizeToSend.
        // It can exceed by at most one item's worth (the item that pushed it over the limit,
        // plus the "always send at least one item" rule).
        const long allowedBatchSize = maxSizeToSendBytes + attachmentSize;
        long maxBatchSize = completedBatches.Max(s => s.BatchSizeInBytes!.Value);

        Assert.True(maxBatchSize <= allowedBatchSize,
            $"Expected batch size to respect MaxSizeToSend ({new Size(maxSizeToSendBytes, SizeUnit.Bytes)}) " +
            $"with at most one attachment overshoot ({new Size(attachmentSize, SizeUnit.Bytes)}), " +
            $"but the largest batch was {new Size(maxBatchSize, SizeUnit.Bytes)}. " +
            $"Total batches: {completedBatches.Count}");
        return;

        static string GetDocId(int i) => $"items/{i}";
        
        static string GetAttachmentId(int i) => $"attachment-{i}";
        
        static byte[] GetAttachmentData(int attachmentSize, int i)
        {
            var data = new byte[attachmentSize];
            new Random(i).NextBytes(data);
            return data;
        }
    }
}
