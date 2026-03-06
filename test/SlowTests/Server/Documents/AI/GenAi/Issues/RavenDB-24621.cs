using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24621(ITestOutputHelper output) : RavenTestBase(output)
{
    public record Item(ImageDescription[] ImageDescriptions = null);
    
    public record ImageDescription(string Description, bool SafeForWork, string[] Tags);

    // TODO: Things to test
    //  - missing attachment
    //  - updating attachment - see that it updates
    //  - removing an attachment - what does it do?
    //  - pdf reading
    //  - jpeg / gif as well
    //  - need specific model for vision? 
    //  - test with multiple attachments
    //  - withJpeg('image.jpg') <-- should validate that this is an error (not base64)
    //  - withJpeg(this.ImageBase64) <-- should work
    
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanUseModelToDescribeImages(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images";
        config.Collection = "Items";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            ImageDescriptions = new[]
            {
                new {
                    Description = "Detailed description of the image", 
                    SafeForWork = true,
                    Tags = new[]{"matching tags for the image"}
                }
            }
        });
        config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
ai.genContext({})
    .withPng(loadAttachment('image.png'));
"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Item(null), "items/1");
            session.Advanced.Attachments.Store("items/1", "image.png", new MemoryStream(Convert.FromBase64String(Data.HeartPngBase64)));
            await session.SaveChangesAsync();
        }
        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120 )));
        using (var session = store.OpenAsyncSession())
        {
            var item = await session.LoadAsync<Item>("items/1");
            Assert.NotNull(item.ImageDescriptions);
        }
    }
    
    
    private record Summary(string Category, decimal TotalSpent, int TransactionCount, string Notes);

    private record Transaction(string User, DateTime Date, string Location, Summary[] Summary = null);
    
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM | RavenAiIntegration.OpenAi | RavenAiIntegration.Google, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanAttachTextFile(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Categorize the expenses in the associated file";
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
            Script = @"
ai.genContext({
    Date: this.Date,
    Location: this.Location,
})
    .withText(loadAttachment('transactions.csv'));
"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        
        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Transaction("users/1", new DateTime(2025, 1, 1), "New York"), "txs/2025-01-01");
            session.Advanced.Attachments.Store("txs/2025-01-01", "transactions.csv", new MemoryStream(Csv.ToArray()));
            await session.SaveChangesAsync();
        }
        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120 )));
        using (var session = store.OpenAsyncSession())
        {
            Transaction tx = await session.LoadAsync<Transaction>("txs/2025-01-01");
            Assert.NotNull(tx.Summary);
        }

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
