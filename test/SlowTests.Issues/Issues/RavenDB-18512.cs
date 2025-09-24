using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18512 : ReplicationTestBase
{
    public RavenDB_18512(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Attachments | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task WillDeDuplicateAttachmentsOverTheWire(Options options)
    {
        var data = new byte[1024 * 16];
        Random.Shared.NextBytes(data);

        using var store1 = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings["Replication.MaxItemsCount"] = "1";
                options.ModifyDatabaseRecord?.Invoke(record);
            }
        });
        using var store2 = GetDocumentStore(options);

        int count = 1;
        for (int i = 0; i < 10; i++)
        {
            using var session1 = store1.OpenAsyncSession();
            await session1.StoreAsync(new { }, $"items/{count}$items/1");
            session1.Advanced.Attachments.Store($"items/{count}$items/1", "attachment-" + count, new MemoryStream(data));
            await session1.SaveChangesAsync();
            count++;
        }

        var database = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "items/1");
        int attachmentsSent = 0;
        database.ReplicationLoader.AttachmentStreamsReceived += (handler, i) =>
        {
            Interlocked.Add(ref attachmentsSent, i);
        };

        await SetupReplicationAsync(store1, store2);
        await EnsureReplicatingAsync(store1, store2);

        Assert.Equal(1, attachmentsSent);
    }
}
