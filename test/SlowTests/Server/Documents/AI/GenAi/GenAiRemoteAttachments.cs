using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using SlowTests.Server.Documents.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiRemoteAttachments(ITestOutputHelper output) : RemoteAttachmentsAzureBase(output)
{
    private const string NonEmptyAnswerHint =
        " ;Always provide a valid structured response matching the schema (if you have no answer or an empty answer - please return default values instead)";


    private record Post(string Content, Comment[] Comments = null);

    private record Comment(string Id, string Author, string Content, string AuthorDescription, string ProfileImage);

    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldBeAbleToUseRemoteAttachments(Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                // Configure attachments as remote
                string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

                // Set the AI connection string
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

                // Set the data
                var marker = "None" + Guid.NewGuid();

                const string postId = "Post/1";

                using (var session = store.OpenAsyncSession())
                {
                    var p1 = new Post("Hello World!",
                    [
                        new Comment(Id: "Comment1", Author: "Shahar Heart", AuthorDescription: marker, Content: "Hey!", ProfileImage: "heart.png"),
                        new Comment(Id: "Comment2", Author: "Omer Star", AuthorDescription: marker, Content: "Hello!", ProfileImage: "star.png"),
                        new Comment(Id: "Comment3", Author: "Aviv Rachmany", AuthorDescription: marker, Content: "Hello", ProfileImage: "none.png")
                    ]);
                    
                    await session.StoreAsync(p1, postId);

                    using var heart = new MemoryStream(Convert.FromBase64String(Data.HeartPngBase64));
                    using var star = new MemoryStream(Convert.FromBase64String(Data.StarPngBase64));

                    var attachments = session.Advanced.Attachments;

                    RemoteAttachmentParameters remote = new(remoteId, DateTime.UtcNow.AddMinutes(1));

                    attachments.Store(postId, new StoreAttachmentParameters("heart.png", heart) { RemoteParameters = remote });
                    attachments.Store(postId, new StoreAttachmentParameters("star.png", star) { RemoteParameters = remote });

                    await session.SaveChangesAsync();
                }
                
                // Force upload
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                
                // Move in time & start remote
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);
                
                await AddGenAiTask(config, store);

                // Bump the document so that it's reprocessed
                using (IAsyncDocumentSession session = store.OpenAsyncSession())
                {
                    var post = await session.LoadAsync<Post>(postId);
                    var metadata = session.Advanced.GetMetadataFor(post);
                    metadata["@Custom"] = "just to update";
                    await session.SaveChangesAsync();
                }

                var etl = Etl.WaitForEtlToComplete(store);

                // Wait for etl to complete
                Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

                // Load it
                using (var session = store.OpenAsyncSession())
                {
                    var p1 = await session.LoadAsync<Post>(postId);
                    var comments = p1.Comments;
                    Assert.NotEqual(marker, comments[0].AuthorDescription);
                    Assert.NotEqual(marker, comments[1].AuthorDescription);
                    Assert.Equal(marker, comments[2].AuthorDescription);
                }

                var hashes = (await GetHashes<Post>(store, postId)).ToList();
                Assert.Equal(2, hashes.Count);

                var etlProcess = database.EtlLoader.Processes.OfType<GenAiTask>().Single();
                var stats = etlProcess.GetPerformanceStats()
                    .Where(x => x.NumberOfLoadedItems > 0)
                    .ToArray();

                var etlLoad = stats[0].Details.Operations
                    .OfType<GenAiPerformanceOperation>()
                    .Single(x => x.Name == EtlOperations.Load);

                var genAiLoadToModel = etlLoad.Operations
                    .OfType<GenAiPerformanceOperation>()
                    .Single(x => x.Name == GenAiOperations.LoadToModel);

                var deferredAttachments = genAiLoadToModel.Operations
                    .OfType<GenAiPerformanceOperation>()
                    .Single(x => x.Name == GenAiOperations.LoadToModelRemoteAttachments);

                Assert.True(deferredAttachments.DurationInMs > 1, "Reaching over the wire is at least 1ms");
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Ai | RavenTestCategory.Attachments)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task ShouldUseCachedResultGenAiResultWhenAttachmentsBecomeRemote(Options options, GenAiConfiguration config)
    {
        await using (CreateCloudSettings())
        {
            using (var store = GetDocumentStore())
            {
                // Configure attachments as remote
                string remoteId = await PutRemoteAttachmentsConfiguration(store, Settings);

                // Set the AI connection string
                await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

                // Enable GenAi task before storing docs
                await AddGenAiTask(config, store);
                
                // Set the data
                var marker = "None" + Guid.NewGuid();

                const string postId = "Post/1";

                using (var session = store.OpenAsyncSession())
                {
                    var p1 = new Post("Hello World!",
                    [
                        new Comment(Id: "Comment1", Author: "Shahar Heart", AuthorDescription: marker, Content: "Hey!", ProfileImage: "heart.png"),
                        new Comment(Id: "Comment2", Author: "Omer Star", AuthorDescription: marker, Content: "Hello!", ProfileImage: "star.png"),
                        new Comment(Id: "Comment3", Author: "Aviv Rachmany", AuthorDescription: marker, Content: "Hello", ProfileImage: "none.png")
                    ]);
                    
                    await session.StoreAsync(p1, postId);

                    using var heart = new MemoryStream(Convert.FromBase64String(Data.HeartPngBase64));
                    using var star = new MemoryStream(Convert.FromBase64String(Data.StarPngBase64));

                    var attachments = session.Advanced.Attachments;

                    RemoteAttachmentParameters remote = new(remoteId, DateTime.UtcNow.AddMinutes(1));

                    attachments.Store(postId, new StoreAttachmentParameters("heart.png", heart) { RemoteParameters = remote });
                    attachments.Store(postId, new StoreAttachmentParameters("star.png", star) { RemoteParameters = remote });

                    await session.SaveChangesAsync();
                }
                
                var etl = Etl.WaitForEtlToComplete(store);

                // Wait for etl to complete
                Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));
                
                // Force upload
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                
                // Move in time & start remote
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);
                
                // Wait for ETL to rerun
                Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(Debugger.IsAttached ? 1200 : 120)));

                var genAi = database.EtlLoader.Processes.OfType<GenAiTask>().Single();
                var stats = genAi.GetPerformanceStats()
                    .Where(x => x.NumberOfLoadedItems > 0)
                    .ToArray();
                
                var last = stats[^1].Details.Operations[^1].Operations
                    .FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
                
                Assert.Equal(2, last.TotalCachedContexts);
                Assert.Equal(0, last.TotalSentToModel);
            }
        }
    }

    private static async Task AddGenAiTask(GenAiConfiguration config, DocumentStore store)
    {
        // Configure AI
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
