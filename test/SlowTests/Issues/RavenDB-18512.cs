using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Incoming;
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

        using var store1 = GetDocumentStore();
        using var store2 = GetDocumentStore();

        using (var session1 = store1.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                await session1.StoreAsync(new { }, "items/" + i);
                session1.Advanced.Attachments.Store("items/" + i, "attachment-" + i, new MemoryStream(data));
            }

            await session1.StoreAsync(new { }, "marker");
            await session1.SaveChangesAsync();
        }

        await SetupReplicationAsync(store1, store2);

        Assert.True(WaitForDocument(store2, "marker"));

        DocumentDatabase database = await GetDatabase(store2.Database);
        WaitForUserToContinueTheTest(store2);
        IncomingReplicationHandler replicationHandler = database.ReplicationLoader.IncomingHandlers.Single();
        var performance = replicationHandler.GetReplicationPerformance();
        int attachmentsSent = performance.Sum(x=>x.Network.AttachmentReadCount);
        Assert.Equal(1, attachmentsSent);
    }
}
