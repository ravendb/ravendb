using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24642(ITestOutputHelper output) : RavenTestBase(output)
{

    public record Item(ImageDescription[] ImageDescriptions = null);

    public record ImageDescription(string Description, bool SafeForWork, string[] Tags);

    private const string HeartPngBase64 =
        "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsMAAA7DAcdvqGQAAAGHaVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8P3hwYWNrZXQgYmVnaW49J++7vycgaWQ9J1c1TTBNcENlaGlIenJlU3pOVGN6a2M5ZCc/Pg0KPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyI+PHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj48cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0idXVpZDpmYWY1YmRkNS1iYTNkLTExZGEtYWQzMS1kMzNkNzUxODJmMWIiIHhtbG5zOnRpZmY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vdGlmZi8xLjAvIj48dGlmZjpPcmllbnRhdGlvbj4xPC90aWZmOk9yaWVudGF0aW9uPjwvcmRmOkRlc2NyaXB0aW9uPjwvcmRmOlJERj48L3g6eG1wbWV0YT4NCjw/eHBhY2tldCBlbmQ9J3cnPz4slJgLAAAA1ElEQVRYR+2WORLDIAxF5Rwhvcvc/0Ap0+cKpMID4gtJmK3IqzwYfb2Rl+EIIQRayIMvzObQJvA9X9f18/PO7kl4akSBNATBg737I1BAC4vEUOt+AiKFgCesBS6QvYSjmxPosfwruAS42UjSXvtMYBV/gX0E+A9iJGmvfSawikxgxmPgPfaaAAHDnqDsQmA2UACZ3kXKhAJUKWihliUKkFJoRcuoCpAhoIalVhUgYxDHWmMSIEcgOfcWp2IL0vHN0zhinkAKaoTWLDRNoCdNE+jJcoEf1VNdHhBR9pYAAAAASUVORK5CYII=";

    private const string StarPngBase64 =
        "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";

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

    private const string NonEmptyAnswerHint =
        " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";


    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single,
        Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single,
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
                new { Description = "Detailed description of the image", SafeForWork = true, Tags = new[] { "matching tags for the image" } }
            }
        });
        config.UpdateScript = @"this.ImageDescriptions = $output.ImageDescriptions;";
        config.GenAiTransformation = new GenAiTransformation { Script = withNullAttachments ? PngScript : PngScriptWithoutNull };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var marker = new ImageDescription("None" + Guid.NewGuid(), false, new string[] { "None" });
        var markerArr = new ImageDescription[] { marker };

        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Item(markerArr), "items/1");
            await session.StoreAsync(new Item(markerArr), "items/2");
            var doc3 = new Item(markerArr);
            await session.StoreAsync(doc3, "Doc/3");
            session.Advanced.GetMetadataFor(doc3)["@collection"] = "Docs"; // store doc3 in another collection

            using var file1 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var file2 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));

            session.Advanced.Attachments.Store("items/1", "image.png", file1);
            session.Advanced.Attachments.Store("Doc/3", "image.png", file2);
            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        using (var session = store.OpenAsyncSession())
        {
            var item1 = await session.LoadAsync<Item>("items/1");
            var item2 = await session.LoadAsync<Item>("items/2");
            var doc3 = await session.LoadAsync<Item>("Doc/3");
            Assert.NotNull(item1.ImageDescriptions);
            Assert.NotNull(item2.ImageDescriptions);
            Assert.NotNull(doc3.ImageDescriptions);
            Assert.True(doc3.ImageDescriptions.Any(d => d.Description == marker.Description)); // shouldn't change - because it's not in 'Items' collection

            Assert.False(item1.ImageDescriptions.Any(d => d.Description == marker.Description));
            var item2Changed = item2.ImageDescriptions.Any(d => d.Description == marker.Description) == false;
            if (withNullAttachments)
                Assert.True(item2Changed || ValidateErrorNotification(db, "The request was refused by the model"));
            else
                Assert.False(item2Changed);
        }

        // Update/delete - Assert hashes
        await AssertHashes<Item>(store, "items/1", "items/2", "image.png", () => new MemoryStream(Convert.FromBase64String(StarPngBase64)), withNullAttachments);
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

    private static ReadOnlySpan<byte> Csv2 => @"Date,Description,Category,Amount
2025-01-01,Grocery Store,Food,45.32
2025-01-02,Utility Bill,Utilities,120.75
2025-01-03,Online Shopping,Retail,89.99
2025-01-04,Gas Station,Transportation,35.50
2025-01-05,Restaurant,Food,62.10
2025-01-06,Internet Bill,Utilities,79.99"u8;

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single,
        Data = new object[] { true })]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single,
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
                new
                {
                    Category = "Expense category (food | entertainment | utilities | education)",
                    TotalSpent = 10m,
                    TransctionCount = 5,
                    Notes =
                        "General observations on this expense category based on the actual expenses (spend too much on takeout or fees are high on utility, etc)"
                }
            }
        });
        config.UpdateScript = @"this.Summary = $output.Summary;";
        config.GenAiTransformation = new GenAiTransformation { Script = withNullAttachments ? Script : ScriptWithoutNull };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var marker = new Summary("None", 0, 0, "None" + Guid.NewGuid());
        var markerArray = new Summary[] { marker };

        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Transaction("Transaction/1", new DateTime(2025, 1, 1), "New York", markerArray), "Transaction/1");
            await session.StoreAsync(new Transaction("Transaction/2", new DateTime(2025, 1, 2), "New York", markerArray), "Transaction/2");
            var doc3 = new Transaction("doc/3", new DateTime(2025, 1, 1), "New York", markerArray);
            await session.StoreAsync(doc3, "Doc/3");
            session.Advanced.GetMetadataFor(doc3)["@collection"] = "Docs"; // store doc3 in another collection

            using var file1 = new MemoryStream(Csv.ToArray());
            using var file2 = new MemoryStream(Csv.ToArray());

            session.Advanced.Attachments.Store("Transaction/1", "transactions.csv", file1);
            session.Advanced.Attachments.Store("Doc/3", "transactions.csv", file2);
            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        using (var session = store.OpenAsyncSession())
        {
            var transaction1 = await session.LoadAsync<Transaction>("Transaction/1");
            var transaction2 = await session.LoadAsync<Transaction>("Transaction/2");
            var doc3 = await session.LoadAsync<Transaction>("Doc/3");
            Assert.NotNull(transaction1.Summary);
            Assert.NotNull(transaction2.Summary);
            Assert.NotNull(doc3.Summary);
            Assert.True(doc3.Summary.Any(d => d.Notes == marker.Notes)); // shouldn't change - because it's not in 'Transaction' collection

            Assert.False(transaction1.Summary.Any(d => d.Notes == marker.Notes));
            var transaction2Changed = transaction2.Summary.Any(d => d.Notes == marker.Notes) == false;
            if (withNullAttachments)
                Assert.True(transaction2Changed || ValidateErrorNotification(db, "The request was refused by the model"));
            else
                Assert.False(transaction2Changed);
        }

        // Update/delete - Assert hashes
        await AssertHashes<Item>(store, "Transaction/1", "Transaction/2", "transactions.csv", () => new MemoryStream(Csv2.ToArray()), withNullAttachments);
    }

    public record Doc(FileDescription[] FileDescriptions = null);

    public record FileDescription(string Description, bool SafeForWork, string[] Tags);

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiDifferentAttachmentsPerContexts(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = @"You are an assistant that receives a list of files and returns structured information about each file.
For each file in the input list:
- Always return one object, in the same order as input.
- If the file is readable and can be processed:
  - Provide a detailed ""Description"" of its contents.
  - Set ""SafeForWork"" to true if the file is appropriate for work, otherwise false.
  - Provide a list of relevant ""Tags"".
- If a file is missing, unreadable, unsupported, or failed to load:
  - Return:
    new { Description = ""Image doesn't exist"", SafeForWork = true, Tags = new[] { ""image doesn't exist"" } }

Your full response should be in this format:

FileDescriptions = new[]
{
    // One object per file
    // Always present all files, even unreadable ones
}
Never skip a file. Return default values for anything unknown or inaccessible.
" + NonEmptyAnswerHint;
        config.Collection = "Docs";
        config.SampleObject = JsonConvert.SerializeObject(new
        {
            FileDescriptions = new[]
            {
                new { Description = "Detailed description of the first file", SafeForWork = true, Tags = new[] { "tag1", "tag2" } },
                new { Description = "Detailed description of the second file", SafeForWork = false, Tags = new[] { "tag3" } },
                new { Description = "Image doesn't exist", SafeForWork = true, Tags = new[] { "image doesn't exist" } }
            }
        });
        config.UpdateScript = @"this.FileDescriptions = $output.FileDescriptions;";
        config.GenAiTransformation = new GenAiTransformation { Script = @"
ai.genContext({})
    .withPng(loadAttachment('heart.png'))
    .withPng(loadAttachment('star.png'))
    .withText(loadAttachment('transactions.csv'));
" };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var marker = new FileDescription("None" + Guid.NewGuid(), false, new string[] { "None" });
        var markerArray = new FileDescription[] { marker };

        var etl = Etl.WaitForEtlToComplete(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Doc(markerArray), "Docs/1");
            await session.StoreAsync(new Doc(markerArray), "Docs/2");

            using var heart1 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var star = new MemoryStream(Convert.FromBase64String(StarPngBase64));
            using var text1 = new MemoryStream(Csv.ToArray());

            using var heart2 = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var text2 = new MemoryStream(Csv.ToArray());


            session.Advanced.Attachments.Store("Docs/1", "heart.png", heart1);
            session.Advanced.Attachments.Store("Docs/1", "star.png", star);
            session.Advanced.Attachments.Store("Docs/1", "transactions.csv", text1);

            session.Advanced.Attachments.Store("Docs/2", "heart.png", heart2);
            session.Advanced.Attachments.Store("Docs/2", "transactions.csv", text2);

            await session.SaveChangesAsync();

        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 240)));

        var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        using (var session = store.OpenAsyncSession())
        {
            var item1 = await session.LoadAsync<Doc>("Docs/1");
            var item2 = await session.LoadAsync<Doc>("Docs/2");
            Assert.NotNull(item1.FileDescriptions);
            Assert.NotNull(item2.FileDescriptions);

            Assert.False(item1.FileDescriptions.Any(d => d.Description == marker.Description));
            var item2Changed = item2.FileDescriptions.Any(d => d.Description == marker.Description) == false;
            Assert.True(item2Changed || ValidateErrorNotification(db, "The request was refused by the model"));
        }
    }

    public record Post(string Content, Comment[] Comments = null);
    public record Comment(string Id, string Author, string Content, string AuthorDescription, string ProfileImage);

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiMultipleAttachment(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Describe the following images." + NonEmptyAnswerHint;
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(
            new { PhotoDescription = "Description of the photo" });

        config.UpdateScript = @"    
const comment = this.Comments.find(c => c.Id == $input.Id);
comment.AuthorDescription = $output.PhotoDescription;
";

        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    let img = loadAttachment(comment.ProfileImage);
    if(!img)
        continue;
    ai.genContext({Id: comment.Id}).withPng(img);
}"
        };

        await store.Maintenance.SendAsync(new AddGenAiOperation(config));

        var etl = Etl.WaitForEtlToComplete(store);

        var marker = "None" + Guid.NewGuid();

        using (var session = store.OpenAsyncSession())
        {
            var p1 = new Post("Hello World!",
                new Comment[]
                {
                    new Comment(Id: "Comment1", Author: "Shahar Heart", AuthorDescription: marker, Content: "Hey!", ProfileImage: "heart.png"),
                    new Comment(Id: "Comment2", Author: "Omer Star", AuthorDescription: marker, Content: "Hello!", ProfileImage: "star.png"),
                    new Comment(Id: "Comment3", Author: "Aviv Rachmany", AuthorDescription: marker, Content: "Hello", ProfileImage: "none.png")
                });
            await session.StoreAsync(p1, "Post/1");

            using var heart = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            using var star = new MemoryStream(Convert.FromBase64String(StarPngBase64));

            session.Advanced.Attachments.Store("Post/1", "heart.png", heart);
            session.Advanced.Attachments.Store("Post/1", "star.png", star);

            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        using (var session = store.OpenAsyncSession())
        {
            var p1 = await session.LoadAsync<Post>("Post/1");
            var comments = p1.Comments;
            Assert.NotEqual(marker, comments[0].AuthorDescription);
            Assert.NotEqual(marker, comments[1].AuthorDescription);
            Assert.Equal(marker, comments[2].AuthorDescription);
        }

        var hashes = (await GetHashes<Post>(store, "Post/1")).ToList();
        Assert.Equal(2, hashes.Count);

        // Changing attachment
        etl = Etl.WaitForEtlToComplete(store);
        var db = await GetDatabase(store.Database);
        using (var session = store.OpenAsyncSession())
        {
            using var heart = new MemoryStream(Convert.FromBase64String(HeartPngBase64));
            session.Advanced.Attachments.Store("Post/1", "star.png", heart); // changing star to be heart
            await session.SaveChangesAsync();
        }
        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        await WaitForAssertionAsync(async () =>
        {
            var hashesAfterChange = (await GetHashes<Post>(store, "Post/1")).ToList();
            Assert.Equal(2, hashesAfterChange.Count);
            Assert.Equal(hashes[0], hashesAfterChange[0]);
            Assert.NotEqual(hashes[1], hashesAfterChange[1]);
        });

        // Delete attachment
        etl = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.Attachments.Delete("Post/1", "star.png");
            await session.SaveChangesAsync();
        }
        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        await WaitForAssertionAsync(async () =>
        {
            var hashesAfterDelete = (await GetHashes<Post>(store, "Post/1")).ToList();
            Assert.Equal(1, hashesAfterDelete.Count);
            Assert.Equal(hashes[0], hashesAfterDelete[0]);
        });
    }


    private async Task AssertHashes<T>(DocumentStore store,
        string docId1,
        string docId2,
        string fileName,
        Func<MemoryStream> fileStreamFactory,
        bool withNullAttachments)
    {
        // Update/delete - Assert hashes

        var oldHash1 = await GetHash<T>(store, docId1); // doc1 - attachment exist (Heart)
        var oldHash2 = await GetHash<T>(store, docId2); // doc2 - attachment doesn't exist
        Assert.NotNull(oldHash1);
        if (withNullAttachments)
            Assert.NotNull(oldHash2);
        else
            Assert.Null(oldHash2);

        var etl = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenAsyncSession())
        {
            using var file1 = fileStreamFactory();
            using var file2 = fileStreamFactory();

            session.Advanced.Attachments.Store(docId1, fileName, file1); // change
            session.Advanced.Attachments.Store(docId2, fileName, file2); // add new
            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        var hash1 = string.Empty;
        var hash2 = string.Empty;
        await WaitForAssertionAsync(async () =>
        {
            hash1 = await GetHash<T>(store, docId1); // doc1 - attachment exist (Star)
            hash2 = await GetHash<T>(store, docId2); // doc2 - attachment exist (Star)
            Assert.NotNull(hash1);
            Assert.NotNull(hash2);
            Assert.NotEqual(oldHash1, hash1);
            Assert.NotEqual(oldHash2, hash1);
        });


        etl = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenAsyncSession())
        {
            session.Advanced.Attachments.Delete(docId1, fileName);
            await session.SaveChangesAsync();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

        await WaitForAssertionAsync(async () =>
        {
            var newHash1 = await GetHash<T>(store, docId1);
        if (withNullAttachments)
            Assert.NotEqual(hash1, newHash1); // doc1 - hash of 'non-existed image.png'
        else
            Assert.Null(newHash1); // doc1 produces no context objects now - metadata hashes gets cleared
        });
    }

    private static async Task<string> GetHash<T>(DocumentStore store, string id)
    {
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<T>(id);
            var metadata = session.Advanced.GetMetadataFor(doc);
            if (metadata.TryGetValue(Constants.Documents.Metadata.GenAiHashes, out object hashesSectionObj) &&
                hashesSectionObj is MetadataAsDictionary hashesSection &&
                hashesSection.TryGetValue("openai-aiintegrationtask", out object hashesObj)
                && hashesObj is IEnumerable<object> hashesArr && hashesArr.First() is string hash)
            {
                return hash;
            }

            return null;
        }
    }

    private static async Task<IEnumerable<string>> GetHashes<T>(DocumentStore store, string id)
    {
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<T>(id);
            var metadata = session.Advanced.GetMetadataFor(doc);
            if (metadata.TryGetValue(Constants.Documents.Metadata.GenAiHashes, out object hashesSectionObj) &&
                hashesSectionObj is MetadataAsDictionary hashesSection &&
                hashesSection.TryGetValue("openai-aiintegrationtask", out object hashesObj)
                && hashesObj is IEnumerable<object> hashesArr)
            {
                return hashesArr.Select(o => o as string);
            }

            return null;
        }
    }
}
