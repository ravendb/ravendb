using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24642(ITestOutputHelper output) : RavenTestBase(output)
{

    public record Item(ImageDescription[] ImageDescriptions = null);

    public record ImageDescription(string Description, bool SafeForWork, string[] Tags);

    private const string HeartPngBase64 = "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAAGHaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8P3hwYWNrZXQgYmVnaW49J++7vycgaWQ9J1c1TTBNcENlaGlIenJlU3pOVGN6a2M5ZCc/Pg0KPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyI+PHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj48cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0idXVpZDpmYWY1YmRkNS1iYTNkLTExZGEtYWQzMS1kMzNkNzUxODJmMWIiIHhtbG5zOnRpZmY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vdGlmZi8xLjAvIj48dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPjwvcmRmOkRlc2NyaXB0aW9uPjwvcmRmOlJERj48L3g6eG1wbWV0YT4NCjw/eHBhY2tldCBlbmQ9J3cnPz4slJgLAAAA1ElEQVRYR+2WORLDIAxF5Rwhvcvc/0Ap0+cKpMID4gtJmK3IqzwYfb2Rl+EIIQRayIMvzObQJvA9X9f18/PO7kl4akSBNATBg737I1BAC4vEUOt+AiKFgCesBS6QvYSjmxPosfwruAS42UjSXvtMYBV/gX0E+A9iJGmvfSawikxgxmPgPfaaAAHDnqDsQmA2UACZ3kXKhAJUKWihliUKkFJoRcuoCpAhoIalVhUgYxDHWmMSIEcgOfcWp2IL0vHN0zhinkAKaoTWLDRNoCdNE+jJcoEf1VNdHhBR9pYAAAAASUVORK5CYII=";

    private const string PngScript = @"
ai.genContext({})
    .withPng(loadAttachment('image.png'));
";

    public const string PngScriptWithoutNull = @"
const img1 = loadAttachment('image.png');
if (!img1) {
    return;
}

ai.genContext({}).withPng(img1);
";

    private const string NonEmptyAnswerHint = " ;Always provide a valid structured response matching the schema (if you have an empty answer - please return default values instead)";


    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false,
        Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false,
        Data = new object[] { false })]
    public async Task CanProcessNonExistedImageAttachment(Options options, GenAiConfiguration config, bool withNullAttachments)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images." + NonEmptyAnswerHint;
        config.Collection = "Items";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            ImageDescriptions = new[]
            {
                new { 
                    Description = "Detailed description of the image", 
                    SafeForWork = true, 
                    Tags = new[]
                    {
                        "matching tags for the image"
                    }
                }
            }
        });
        config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";
        config.GenAiTransformation = new GenAiTransformation { Script = withNullAttachments ? PngScript : PngScriptWithoutNull };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var noneDescription = "None" + Guid.NewGuid();
        var none = new ImageDescription[] { new ImageDescription(noneDescription, false, new string[] { "None" }) };

        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Item(none), "items/1");
            await session.StoreAsync(new Item(none), "items/2");
            var doc3 = new Item(none);
            await session.StoreAsync(doc3, "Doc/3");
            session.Advanced.GetMetadataFor(doc3)["@collection"] = "Docs"; // store doc3 in another collection

            using var file1 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var file2 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));

            session.Advanced.Attachments.Store("items/1", "image.png", file1);
            session.Advanced.Attachments.Store("Doc/3", "image.png", file2);
            await session.SaveChangesAsync();
        }

         Assert.True(etl.Wait(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        await WaitForAssertionAsync(async () =>
        {
            using (var session = store.OpenAsyncSession())
            {
                var item1 = await session.LoadAsync<Item>("items/1");
                var item2 = await session.LoadAsync<Item>("items/2");
                var doc3 = await session.LoadAsync<Item>("Doc/3");
                Assert.NotNull(item1.ImageDescriptions);
                Assert.NotNull(item2.ImageDescriptions);
                Assert.NotNull(doc3.ImageDescriptions);
                Assert.True(doc3.ImageDescriptions.Any(d => d.Description == noneDescription)); // shouldn't change - because it's not in 'Items' collection

                Assert.False(item1.ImageDescriptions.Any(d => d.Description == noneDescription));
                var item2Changed = item2.ImageDescriptions.Any(d => d.Description == noneDescription) == false;
                if (withNullAttachments)
                    Assert.True(item2Changed || ValidateRefusalNotification(db));
                else
                    Assert.False(item2Changed);
            }
        }, TimeSpan.FromSeconds(15));
    }

    private static bool ValidateRefusalNotification(DocumentDatabase db)
    {
        using (db.NotificationCenter.GetStored(out var actions))
        {
            var jsonAlerts = actions.ToList();
            if (jsonAlerts.Count == 0)
                return false;
            var bjro = jsonAlerts.First().Json;

            return bjro.TryGet("Details", out BlittableJsonReaderObject details) &&
                details.TryGet("Errors", out BlittableJsonReaderArray errors) &&
                errors.Length > 0 &&
                errors.First().ToString()!.Contains("The request was refused by the model");
        }
    }

    private record Summary(string Category, decimal TotalSpent, int TransactionCount, string Notes);

    private record Transaction(string User, DateTime Date, string Location, Summary[] Summary = null);


    private const string Script =
        @"
ai.genContext({
    Date: this.Date,
    Location: this.Location,
})
    .withText(loadAttachment('transactions.csv'));
";

    public const string ScriptWithoutNull = @"
const file = loadAttachment('transactions.csv');
if (!file) {
    return;
}

ai.genContext({
    Date: this.Date,
    Location: this.Location,
}).withText(file);
";

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false,
        Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false,
        Data = new object[] { false })]
    public async Task CanProcessNonExistedTextAttachment(Options options, GenAiConfiguration config, bool withNullAttachments)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Categorize the expenses in the associated file. " + NonEmptyAnswerHint;
        config.Collection = "Transactions";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            Summary = new[]
            {
                new {
                    Category = "Expense category (food | entertainment | utilities | education)",
                    TotalSpent = 10m,
                    TransctionCount = 5,
                    Notes = "General observations on this expense category based on the actual expenses (spend too much on takeout or fees are high on utility, etc)"
                }
            }
        });
        config.UpdateScript = @"this.Summary = $output.Summary;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = withNullAttachments ? Script : ScriptWithoutNull
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var noneNotes = "None" + Guid.NewGuid();
        var none = new Summary[] { new Summary("None", 0, 0, noneNotes) };

        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Transaction("Transaction/1", new DateTime(2025, 1, 1), "New York", none), "Transaction/1");
            await session.StoreAsync(new Transaction("Transaction/2", new DateTime(2025, 1, 2), "New York", none), "Transaction/2");
            var doc3 = new Transaction("doc/3", new DateTime(2025, 1, 1), "New York", none);
            await session.StoreAsync(doc3, "Doc/3");
            session.Advanced.GetMetadataFor(doc3)["@collection"] = "Docs"; // store doc3 in another collection

            using var file1 = new MemoryStream(Csv.ToArray());
            using var file2 = new MemoryStream(Csv.ToArray());

            session.Advanced.Attachments.Store("Transaction/1", "transactions.csv", file1);
            session.Advanced.Attachments.Store("Doc/3", "transactions.csv", file2);
            await session.SaveChangesAsync();
        }

        Assert.True(etl.Wait(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        await WaitForAssertionAsync(async () =>
        {
            using (var session = store.OpenAsyncSession())
            {
                var transaction1 = await session.LoadAsync<Transaction>("Transaction/1");
                var transaction2 = await session.LoadAsync<Transaction>("Transaction/2");
                var doc3 = await session.LoadAsync<Transaction>("Doc/3");
                Assert.NotNull(transaction1.Summary);
                Assert.NotNull(transaction2.Summary);
                Assert.NotNull(doc3.Summary);
                Assert.True(doc3.Summary.Any(d => d.Notes == noneNotes)); // shouldn't change - because it's not in 'Transaction' collection

                Assert.False(transaction1.Summary.Any(d => d.Notes == noneNotes));
                var transaction2Changed = transaction2.Summary.Any(d => d.Notes == noneNotes) == false;
                if (withNullAttachments)
                    Assert.True(transaction2Changed || ValidateRefusalNotification(db));
                else
                    Assert.False(transaction2Changed);
            }
        }, TimeSpan.FromSeconds(15));


    }

    private static ReadOnlySpan<byte> Csv => @"Date,Description,Category,Amount
2025-01-01,Grocery Store,Food,45.32
2025-01-02,Utility Bill,Utilities,120.75
2025-01-03,Online Shopping,Retail,89.99
2025-01-04,Gas Station,Transportation,35.50
2025-01-05,Restaurant,Food,62.10
2025-01-06,Internet Bill,Utilities,79.99
2025-01-07,Pharmacy,Health,22.45
2025-01-08,Streaming Service,Entertainment,14.99
2025-01-09,Gym Membership,Fitness,40.00
2025-01-10,Clothing Store,Retail,75.20
2025-01-11,Coffee Shop,Food,8.75
2025-01-12,Car Insurance,Transportation,95.00
2025-01-13,Home Depot,Home,130.25
2025-01-14,Pet Supplies,Pet Care,28.60
2025-01-15,Doctor Visit,Health,50.00
2025-01-16,Grocery Store,Food,53.80
2025-01-17,Movie Theater,Entertainment,22.00
2025-01-18,Phone Bill,Utilities,65.30
2025-01-19,Bookstore,Retail,19.95
2025-01-20,Auto Repair,Transportation,210.50
2025-01-21,Fast Food,Food,12.65
2025-01-22,Charity Donation,Miscellaneous,25.00
2025-01-23,Hair Salon,Personal Care,45.00
2025-01-24,Grocery Store,Food,60.15
2025-01-25,Online Subscription,Entertainment,9.99"u8;

}
