using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiStats(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiStats_ShouldReport_ModelCallStats(Options options, GenAiConfiguration configuration)
    {
        using var store = GetDocumentStore();

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));

        var etlDone = Etl.WaitForEtlToComplete(store);

        configuration.Prompt = "Check if the following blog post comment is spam or not";
        configuration.Collection = "Posts";
        configuration.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
        configuration.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].IsSpam = $output.Blocked;
";
        configuration.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));


        var db = await GetDatabase(store.Database);

        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        const string docId = "posts/1";

        var post = new GenAiBasics.Post([
            new GenAiBasics.Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
            new GenAiBasics.Comment("Great article. Helped me understand indexing in RavenDB.", "alex"),
            new GenAiBasics.Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
        ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful...");

        using (var session = store.OpenSession())
        {
            session.Store(post, docId);
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

        var stats = etlProcess.GetPerformanceStats()
            .Where(x => x.NumberOfLoadedItems > 0)
            .ToArray();

        Assert.Equal(1, stats[0].NumberOfExtractedItems[EtlItemType.Document]);

        var loadDetails = stats[0].Details.Operations[^1];

        Assert.Equal("Load", loadDetails.Name);

        var genAiStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.NotNull(genAiStats);

        Assert.Equal(3, genAiStats.NumberOfContextObjects);
        Assert.Equal(0, genAiStats.TotalCachedContexts);
        Assert.Equal(3, genAiStats.TotalSentToModel);

        Assert.True(genAiStats.Usage.CompletionTokens > 0);
        Assert.True(genAiStats.Usage.PromptTokens > 0);
        var expectedTotalTokens = genAiStats.Usage.CompletionTokens + genAiStats.Usage.PromptTokens;
        Assert.Equal(expectedTotalTokens, genAiStats.Usage.TotalTokens);

        using (var session = store.OpenAsyncSession())
        {
            etlDone = Etl.WaitForEtlToComplete(store);

            // add a new comment

            var doc = await session.LoadAsync<GenAiBasics.Post>(docId);
            doc.Comments.Add(new GenAiBasics.Comment("new spam comment", "evil hacker"));

            var etag = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(doc), db.DbBase64Id);

            await session.SaveChangesAsync();

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            EtlPerformanceStats[] stats2 = null;

            var value = await WaitForValueAsync(() =>
            {
                stats2 = etlProcess.GetPerformanceStats()
                    .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag == etag + 1)
                    .ToArray();
                return stats2.Length > 0;
            }, true, timeout: 60_000);

            Assert.True(value);
            Assert.Equal(1, stats2[^1].NumberOfExtractedItems[EtlItemType.Document]);

            var loadDetails2 = stats2[^1].Details.Operations[^1];

            Assert.Equal("Load", loadDetails2.Name);

            var genAiStats2 = loadDetails2.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
            Assert.NotNull(genAiStats2);

            // only the newly added comment should be sent to model, the rest should be cached
            Assert.Equal(4, genAiStats2.NumberOfContextObjects);
            Assert.Equal(3, genAiStats2.TotalCachedContexts);
            Assert.Equal(1, genAiStats2.TotalSentToModel);

            Assert.True(genAiStats2.Usage.CompletionTokens > 0);
            Assert.True(genAiStats2.Usage.PromptTokens > 0);
            Assert.True(genAiStats2.Usage.TotalTokens > 0);
        }
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiStats_Should_Report_TransformationStats(Options options, GenAiConfiguration configuration)
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
        var etlDone = Etl.WaitForEtlToComplete(store);

        configuration.Prompt = "Check if the following blog post comment is spam or not";
        configuration.Collection = "Posts";
        configuration.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
        configuration.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].IsSpam = $output.Blocked;
";
        configuration.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };
        store.Maintenance.Send(new AddGenAiOperation(configuration));

        const string docId = "posts/1";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("comment A", "u1"), new GenAiBasics.Comment("comment B", "u2")], "title", "body"), docId);
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

        var db = await GetDatabase(store.Database);
        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        // check transformation stats

        EtlPerformanceStats[] stats = null;

        var value = await WaitForValueAsync(() =>
        {
            stats = etlProcess.GetPerformanceStats()
                .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag == 1)
                .ToArray();
            return stats.Length > 0;
        }, expectedVal: true);

        Assert.True(value);

        var transformScope = stats[0].Details.Operations.FirstOrDefault(x => x.Name == EtlOperations.Transform) as GenAiPerformanceOperation;

        Assert.NotNull(transformScope);
        Assert.Equal(2, transformScope.NumberOfContextObjects);
        Assert.Equal(0, transformScope.TotalCachedContexts);

        // in the second batch (after document got updated) everything should be cached

        EtlPerformanceStats[] stats2 = null;

        value = await WaitForValueAsync(() =>
        {
            stats2 = etlProcess.GetPerformanceStats()
                .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag > 1)
                .ToArray();
            return stats2.Length > 0;
        }, expectedVal: true);

        Assert.True(value);

        transformScope = stats2[0].Details.Operations.FirstOrDefault(x => x.Name == EtlOperations.Transform) as GenAiPerformanceOperation;

        Assert.NotNull(transformScope);
        Assert.Equal(2, transformScope.NumberOfContextObjects);
        Assert.Equal(2, transformScope.TotalCachedContexts);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiStats_ShouldReport_UpdatePhaseStats(Options options, GenAiConfiguration configuration)
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));
        var etlDone = Etl.WaitForEtlToComplete(store);

        configuration.Prompt = "Check if the following blog post comment is spam or not";
        configuration.Collection = "Posts";
        configuration.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
        configuration.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].IsSpam = $output.Blocked;
";
        configuration.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        const string docId = "posts/1";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("spam link here", "bot"), new GenAiBasics.Comment("thanks for the article", "alex")], "Title", "Body"), docId);
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

        var db = await GetDatabase(store.Database);

        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        // check stats of first batch
        var stats1 = etlProcess.GetPerformanceStats()
            .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag == 1)
            .ToArray();

        Assert.Equal(1, stats1[0].NumberOfExtractedItems[EtlItemType.Document]);

        var loadDetails = stats1[0].Details.Operations[^1];

        Assert.Equal("Load", loadDetails.Name);

        var updatePhaseStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.ApplyUpdateScript) as GenAiPerformanceOperation;
        Assert.NotNull(updatePhaseStats);

        Assert.Equal(2, updatePhaseStats.NumberOfContextObjects);
        Assert.Equal(2, updatePhaseStats.TotalUpdates);
        Assert.Equal(0, updatePhaseStats.TotalCachedContexts);
        Assert.Equal(0, updatePhaseStats.UpdateFailures);

        // check stats of second batch (after the document is updated ETL is triggered again)
        EtlPerformanceStats[] stats2 = null;
        var value = await WaitForValueAsync(() =>
        {
            stats2 = etlProcess.GetPerformanceStats()
                .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag > 1)
                .ToArray();
            return stats2.Length > 0;
        }, expectedVal: true);

        Assert.True(value);

        Assert.Equal(1, stats2[0].NumberOfExtractedItems[EtlItemType.Document]);

        loadDetails = stats2[0].Details.Operations[^1];

        Assert.Equal("Load", loadDetails.Name);

        updatePhaseStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.ApplyUpdateScript) as GenAiPerformanceOperation;
        Assert.NotNull(updatePhaseStats);

        // nothing should be patched in this batch, everything is cached
        Assert.Equal(2, updatePhaseStats.NumberOfContextObjects);
        Assert.Equal(0, updatePhaseStats.TotalUpdates);
        Assert.Equal(2, updatePhaseStats.TotalCachedContexts);
        Assert.Equal(0, updatePhaseStats.UpdateFailures);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiStats_ShouldReport_ModelErrorStats(Options options, GenAiConfiguration configuration)
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));

        configuration.Prompt = "this will cause a model error";
        configuration.Collection = "Posts";
        configuration.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "why" });
        configuration.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].IsSpam = $output.Blocked;
";
        configuration.GenAiTransformation = new GenAiTransformation
        {
            Script = @"for (const comment of this.Comments) { ai.genContext({Text: comment.Text}); }"
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        var db = await GetDatabase(store.Database);
        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        // simulate model call failure
        var chatCompletionClient = etlProcess.GetChatCompletionClient();
        chatCompletionClient.ForTestingPurposesOnly().SimulateFailureAsync = (ctx)
            => throw new RateLimitException("rate limit") { RetryAfter = TimeSpan.FromMinutes(30), RequestId = "test" };

        const string docId = "posts/1";
        using (var session = store.OpenSession())
        {
            session.Store(new GenAiBasics.Post([new GenAiBasics.Comment("this will fail", "mock")], "bad", "fail"), docId);
            session.SaveChanges();
        }

        EtlErrorInfo error = null;
        var value = await WaitForValueAsync(async () =>
        {
            error = await Etl.TryGetLoadErrorAsync(store.Database, configuration);
            return error != null;
        }, true, timeout: 60_000);

        Assert.True(value);
        Assert.Contains("rate limit", error?.Error);

        var stats = etlProcess.GetPerformanceStats()
            .Where(x => x.LastExtractedEtags[EtlItemType.Document] > 0)
            .ToArray();

        Assert.Equal(1, stats[0].NumberOfExtractedItems[EtlItemType.Document]);

        var loadDetails = stats[0].Details.Operations[^1];
        Assert.Equal("Load", loadDetails.Name);

        // should report model call failure in LoadToModel stats
        var modelScope = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.NotNull(modelScope);
        Assert.Equal(1, modelScope.NumberOfContextObjects);
        Assert.Equal(1, modelScope.TotalSentToModel);
        Assert.Equal(0, modelScope.TotalCachedContexts);
        Assert.Equal(1, modelScope.ModelCallFailures);

        var updatePhaseStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.ApplyUpdateScript) as GenAiPerformanceOperation;
        Assert.NotNull(updatePhaseStats);

        // nothing should be patched 
        Assert.Equal(1, updatePhaseStats.NumberOfContextObjects);
        Assert.Equal(0, updatePhaseStats.TotalUpdates);
        Assert.Equal(0, updatePhaseStats.TotalCachedContexts);
        Assert.Equal(0, updatePhaseStats.UpdateFailures);
    }

    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAiStats_ShouldReport_UpdateFailuresStats(Options options, GenAiConfiguration configuration)
    {
        using var store = GetDocumentStore();
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(configuration.Connection));

        var badScript =
            @"
if ($input.Id == '1')
    return;
const idx = this.Comments.findIndexf(c => c.Id == $input.Id);  
if ($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";

        configuration.Prompt = "this will cause a model error";
        configuration.Collection = "Posts";
        configuration.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "why" });
        configuration.UpdateScript = badScript;
        configuration.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(configuration));

        var etlDone = Etl.WaitForEtlToComplete(store);

        const string docId = "posts/1";
        using (var session = store.OpenSession())
        {
            var comment1 = new GenAiBasics.Comment("comment1", "me") { Id = "1" };
            var comment2 = new GenAiBasics.Comment("comment2", "me") { Id = "2" };

            session.Store(new GenAiBasics.Post([comment1, comment2], "title", "body"), docId);
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

        var db = await GetDatabase(store.Database);
        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        var stats = etlProcess.GetPerformanceStats()
            .Where(x => x.NumberOfLoadedItems > 0 && x.LastLoadedEtag == 1)
            .ToArray();

        Assert.Equal(1, stats[0].NumberOfExtractedItems[EtlItemType.Document]);

        var loadDetails = stats[0].Details.Operations[^1];
        Assert.Equal("Load", loadDetails.Name);

        var modelScope = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.LoadToModel) as GenAiPerformanceOperation;
        Assert.NotNull(modelScope);
        Assert.Equal(2, modelScope.NumberOfContextObjects);
        Assert.Equal(2, modelScope.TotalSentToModel);
        Assert.Equal(0, modelScope.TotalCachedContexts);
        Assert.Equal(0, modelScope.ModelCallFailures);

        var updatePhaseStats = loadDetails.Operations.FirstOrDefault(x => x.Name == GenAiOperations.ApplyUpdateScript) as GenAiPerformanceOperation;
        Assert.NotNull(updatePhaseStats);

        // should report update script failure in update stats
        Assert.Equal(2, updatePhaseStats.NumberOfContextObjects);
        Assert.Equal(2, updatePhaseStats.TotalUpdates);
        Assert.Equal(0, updatePhaseStats.TotalCachedContexts);
        Assert.Equal(1, updatePhaseStats.UpdateFailures);
    }
}
