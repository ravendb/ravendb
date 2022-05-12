using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18512 : ReplicationTestBase
{
    public RavenDB_18512(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task WillDeDuplicateAttachmentsOverTheWire()
    {
        var data = new byte[1024 * 16];
        Random.Shared.NextBytes(data);

        using var store1 = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = record =>
            {
                record.Settings["Replication.MaxItemsCount"] = "1";
            }
        });
        using var store2 = GetDocumentStore();


        int count = 1;
        for (int i = 0; i < 10; i++)
        {
            using var session1 = store1.OpenAsyncSession();
            await session1.StoreAsync(new { }, "items/" + count);
            session1.Advanced.Attachments.Store("items/" + count, "attachment-" + count, new MemoryStream(data));
            await session1.SaveChangesAsync();
            count++;
        }

        using (var session1 = store1.OpenAsyncSession())
        {
            await session1.StoreAsync(new { }, "marker");

            await session1.SaveChangesAsync();
        }
        DocumentDatabase database = await GetDatabase(store2.Database);
        int attachmentsSent = 0;
        database.ReplicationLoader.AttachmentStreamsReceived += (handler, i) =>
        {
            Interlocked.Add(ref attachmentsSent, i);
        };

        await SetupReplicationAsync(store1, store2);
        WaitForUserToContinueTheTest(store2);

        Assert.True(WaitForDocument(store2, "marker"));

        Assert.Equal(1, attachmentsSent);
    }
}
