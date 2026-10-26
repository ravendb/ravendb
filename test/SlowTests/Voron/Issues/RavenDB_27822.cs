using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Server.Extensions;
using Raven.Server.Storage;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server.Meters;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27822 : RavenTestBase
{
    public RavenDB_27822(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public async Task StorageIo_shows_resolved_compression_and_the_reason_of_the_last_sync()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var env = database.DocumentsStorage.Environment;

        // Auto compression picks Zstd on a bandwidth budgeted disk
        env.Options.ForTestingPurposesOnly().ForceMeasuredDeviceClass = DeviceWriteBudget.DeviceClass.Budgeted;

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Arek" });
            await session.SaveChangesAsync();
        }

        env.Journal.Applicator.ApplyLogsToDataFile(CancellationToken.None, TimeSpan.FromSeconds(30));
        env.ForceSyncDataFile();
        await WaitForValueAsync(() => env.WriteFlow.LastSync?.Reason, (IoMetrics.SyncReason?)IoMetrics.SyncReason.Explicit);

        var report = await GetStorageIoAsync(store);

        var documents = report.Environments.Single(x => x.Database == store.Database && x.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
        Assert.Equal(JournalCompressionAlgorithm.Auto, documents.Compression.Configured);
        Assert.Equal(JournalCompressionAlgorithm.Zstd, documents.Compression.Resolved);
        Assert.Equal(IoMetrics.SyncReason.Explicit, documents.Sync.LastReason);
        Assert.NotNull(documents.Sync.LastAt);

        Assert.Contains(report.Environments, x => x.Database == null && x.Type == StorageEnvironmentWithType.StorageEnvironmentType.System);

        if (documents.DeviceId != null) // null when the platform cannot identify the device
            Assert.Contains(report.Devices, x => x.DeviceId == documents.DeviceId);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public async Task StorageIo_does_not_need_the_write_transaction()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        using var writeTxHeld = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var holder = Task.Run(() =>
        {
            using (database.DocumentsStorage.Environment.WriteTransaction())
            {
                writeTxHeld.Set();
                release.Wait(TimeSpan.FromMinutes(1));
            }
        });

        try
        {
            Assert.True(writeTxHeld.Wait(TimeSpan.FromSeconds(30)));

            var report = await GetStorageIoAsync(store).WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Contains(report.Environments, x => x.Database == store.Database);
        }
        finally
        {
            release.Set();
            await holder;
        }
    }

    private static async Task<StorageIoReport> GetStorageIoAsync(Raven.Client.Documents.IDocumentStore store)
    {
        var requestExecutor = store.GetRequestExecutor();
        var response = await requestExecutor.HttpClient.SendAsync(
            new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/admin/debug/storage/io").WithConventions(store.Conventions));
        response.EnsureSuccessStatusCode();

        return JsonConvert.DeserializeObject<StorageIoReport>(await response.Content.ReadAsStringAsync());
    }
}
