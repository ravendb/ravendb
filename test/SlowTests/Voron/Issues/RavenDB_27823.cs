using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Server.Extensions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server.Meters;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27823 : RavenTestBase
{
    public RavenDB_27823(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public async Task IoMetrics_show_compression_algorithm_merged_batch_and_sync_reason()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false }); // an in-memory data file is never synced
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var env = database.DocumentsStorage.Environment;

        // Auto compression picks Zstd on a bandwidth budgeted disk
        env.Options.ForTestingPurposesOnly().ForceMeasuredDeviceClass = DeviceWriteBudget.DeviceClass.Budgeted;

        using (var session = store.OpenAsyncSession())
        {
            // well above Storage.CompressTxAboveSizeInKb (32 KB), so the transaction is compressed
            await session.StoreAsync(new User { Name = new string('a', 256 * 1024) });
            await session.SaveChangesAsync();
        }

        env.Journal.Applicator.ApplyLogsToDataFile(CancellationToken.None, TimeSpan.FromSeconds(30));
        env.ForceSyncDataFile();
        await WaitForValueAsync(() => env.WriteFlow.LastSync?.Reason, (IoMetrics.SyncReason?)IoMetrics.SyncReason.Explicit);

        var requestExecutor = store.GetRequestExecutor();
        var response = await requestExecutor.HttpClient.SendAsync(
            new HttpRequestMessage(HttpMethod.Get, $"{store.Urls.First()}/databases/{store.Database}/debug/io-metrics").WithConventions(store.Conventions));
        response.EnsureSuccessStatusCode();

        var recent = JObject.Parse(await response.Content.ReadAsStringAsync())["Environments"]
            .Where(e => e.Value<string>("Type") == "Documents")
            .SelectMany(e => e["Files"])
            .SelectMany(f => f["Recent"])
            .ToList();

        Assert.Contains(recent, r => r.Value<string>("Type") == "Compression" && r.Value<string>("Algorithm") == "Zstd");
        Assert.Contains(recent, r => r.Value<string>("Type") == "JournalWrite" && r.Value<int>("BatchOperations") > 0);
        Assert.True(recent.Any(r => r.Value<string>("Type") == "DataSync" && r.Value<string>("SyncReason") == "Explicit"),
            string.Join(Environment.NewLine, recent.Where(r => r.Value<string>("Type") == "DataSync").Select(r => r.ToString(Newtonsoft.Json.Formatting.None))));
    }
}
