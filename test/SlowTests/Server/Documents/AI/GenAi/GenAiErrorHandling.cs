using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiErrorHandling(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_ShouldRaiseAlertOnInvalidContextExtractionScript(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // function 'ai.genContext(ctx)' must be called with a single argument
            var badScript =
                @"for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id}, comment.Hash); 
}";

            config.Prompt = "Check if the following blog post comment is spam or not";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
            config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].IsSpam = $output.Blocked;
";
            config.GenAiTransformation = new GenAiTransformation { Script = badScript };


            store.Maintenance.Send(new AddGenAiOperation(config));

            const string docId = "posts/1";

            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([
                    new GenAiBasics.Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
                    new GenAiBasics.Comment("Great article. Helped me understand indexing in RavenDB.", "alex"),
                    new GenAiBasics.Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
                ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful..."), docId);
                session.SaveChanges();
            }

            EtlErrorInfo error = null;
            var value = await WaitForValueAsync(async () =>
            {
                error = await Etl.TryGetTransformationErrorAsync(store.Database, config);
                return error != null;
            }, true, timeout: 60_000);

            Assert.True(value);
            Assert.NotNull(error);
            Assert.True(error.Error.Contains("Invalid number of arguments for ai.genContext(ctx)"));
            Assert.Equal(docId, error.DocumentId);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_ShouldRaiseAlertOnInvalidUpdateScript(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // function 'context(ctx)' must be called with a single argument
            var badScript =
                @"const idx = this.Comments.findIndexf(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";

            config.Prompt = "Check if the following blog post comment is spam or not";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
            config.UpdateScript = badScript;
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}"
            };


            store.Maintenance.Send(new AddGenAiOperation(config));

            const string docId = "posts/1";

            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([
                    new GenAiBasics.Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
                    new GenAiBasics.Comment("Great article. Helped me understand indexing in RavenDB.", "alex"),
                    new GenAiBasics.Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
                ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful..."), docId);
                session.SaveChanges();
            }

            EtlErrorInfo error = null;
            var value = await WaitForValueAsync(async () =>
            {
                error = await Etl.TryGetLoadErrorAsync(store.Database, config);
                return error != null;
            }, true, timeout: 60_000);

            Assert.True(value, await Etl.GetEtlDebugInfo(store.Database, TimeSpan.FromSeconds(60)));
            Assert.NotNull(error);
            Assert.True(error.Error.Contains("Failed to apply update script"));
            Assert.True(error.Error.Contains("JavaScriptException: Property 'findIndexf' of object is not a function"));
            Assert.Equal(docId, error.DocumentId);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_UpdateScriptErrorsShouldBeHandledPerContext(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            // first comment should manage to apply this update script (noop), second comment should fail
            var badScript =
                @"if($input.Id != '42')
    return;

const idx = this.Comments.findIndexf(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";

            config.Prompt = "Check if the following blog post comment is spam or not";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
            config.UpdateScript = badScript;
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}"
            };

            store.Maintenance.Send(new AddGenAiOperation(config));

            const string docId = "posts/1";

            using (var session = store.OpenSession())
            {
                var post = new GenAiBasics.Post([
                    new GenAiBasics.Comment("Great article. Helped me understand indexing in RavenDB.", "alex"),
                    new GenAiBasics.Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
                ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful...");

                post.Comments[0].Id = "0";
                post.Comments[1].Id = "42";

                session.Store(post, docId);
                session.SaveChanges();
            }

            EtlErrorInfo error = null;
            var value = await WaitForValueAsync(async () =>
            {
                error = await Etl.TryGetLoadErrorAsync(store.Database, config);
                return error != null;
            }, true, timeout: 60_000);

            Assert.True(value);
            Assert.NotNull(error);

            Assert.Contains("Failed to apply update script for context", error.Error);
            Assert.Contains("\"Text\":\"Free crypto airdrop! Sign up now at scamcoin.fake", error.Error);

            using (var session = store.OpenSession())
            {
                var doc = session.Load<BlittableJsonReaderObject>(docId);
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashes));

                Assert.Equal(1, hashes.Count); // only one context update script was successful 
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_LoadError_ModelRefusedToAnswer(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "Check if the following blog post comment is spam or not";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
            config.UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].IsBlocked = $output.Blocked;";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}"
            };
            config.Identifier = "blog-post-spam-check";

            store.Maintenance.Send(new AddGenAiOperation(config));

            var db = await GetDatabase(store.Database);

            var etlDone = Etl.WaitForEtlToComplete(store);

            var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
            Assert.NotNull(etlProcess);

            var chatCompletionClient = etlProcess.GetChatCompletionClient();
            chatCompletionClient.ForTestingPurposesOnly().SimulateFailureAsync = (ctx) =>
            {
                if (ctx.Contains("win $$$$"))
                    throw new RefusedToAnswerException("fake refusal") { RequestId = "fake request id" };

                return Task.CompletedTask;
            };

            const string docId = "posts/1";

            using (var session = store.OpenSession())
            {
                session.Store(new GenAiBasics.Post([
                    new GenAiBasics.Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
                    new GenAiBasics.Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                    new GenAiBasics.Comment("Great article. Helped me understand indexing in RavenDB.", "alex")
                ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful..."), docId);
                session.SaveChanges();
            }

            Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

            EtlErrorInfo error = null;
            var value = await WaitForValueAsync(async () =>
            {
                error = await Etl.TryGetLoadErrorAsync(store.Database, config);
                return error != null;
            }, true, timeout: 60_000);

            Assert.True(value);
            Assert.NotNull(error);

            Assert.Contains("Model call failed", error.Error);
            Assert.Contains("win $$$$", error.Error);
            Assert.Contains("fake refusal", error.Error);

            using (var session = store.OpenSession())
            {
                var doc = session.Load<BlittableJsonReaderObject>(docId);
                Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
                Assert.True(hashesSection.TryGet(config.Identifier, out BlittableJsonReaderArray hashes));

                Assert.Equal(3, hashes.Length); // all 3 context hashes should be in metadata (refusal is considered a success) 
            }

            // assert stats
            var stats = etlProcess.GetPerformanceStats().Last(s => s.NumberOfLoadedItems > 0);
            Assert.True(stats.SuccessfullyLoaded);

            // assert that next ETL batch does not start from etag 0 (batch was successful)
            var state = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
            var lastProcessedEtag = state.GetLastProcessedEtag(db.DbBase64Id, Server.ServerStore.NodeTag);
            Assert.True(lastProcessedEtag > 0);
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_ShouldRespectRateLimitErrorAndFallback(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Translate this text to sanskrit";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Result = "text" });
        config.UpdateScript = "this.Result = $output.Result;";
        config.GenAiTransformation = new GenAiTransformation { Script = "for (const comment of this.Comments) ai.genContext({Text: comment.Text, Id: comment.Id});" };
        config.Identifier = "sanskrit-translation";

        store.Maintenance.Send(new AddGenAiOperation(config));

        var db = await GetDatabase(store.Database);
        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        int triggerOn = 2;
        var chatCompletionClient = etlProcess.GetChatCompletionClient();
        chatCompletionClient.ForTestingPurposesOnly().SimulateFailureAsync = (ctx) =>
        {
            if (ctx.Contains(config.Prompt) || ctx.Contains("AI Agent Parameters:"))
                return Task.CompletedTask;

            if (Interlocked.Decrement(ref triggerOn) <= 0)
                throw new RateLimitException("rate limit") { RetryAfter = TimeSpan.FromMinutes(10), RequestId = "test" };

            return Task.CompletedTask;
        };

        const string docId = "posts/1";
        const string docId2 = "posts/2";

        using (var session = store.OpenSession())
        {
            var post = new GenAiBasics.Post([
                new GenAiBasics.Comment("comment 1", "a"),
                new GenAiBasics.Comment("comment 2", "b"),
                new GenAiBasics.Comment("comment 3", "c"),
            ], "title", "body");

            session.Store(post, docId);
            session.SaveChanges();
        }

        EtlErrorInfo error = null;
        var gotError = await WaitForValueAsync(async () =>
        {
            error = await Etl.TryGetLoadErrorAsync(store.Database, config);
            return error != null;
        }, true, timeout: 60_000);

        Assert.True(gotError && error.Error.Contains("rate limit"), await AddDebugInfo(store, config));

        using (var session = store.OpenSession())
        {
            var doc = session.Load<BlittableJsonReaderObject>(docId);
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashes));
            Assert.True(hashes.TryGet(config.Identifier, out BlittableJsonReaderArray arr));

            Assert.Equal(1, arr.Length); // only some processed
        }

        var stats = etlProcess.GetPerformanceStats().Last();
        Assert.Equal(1, stats.LastExtractedEtags[EtlItemType.Document]);
        Assert.Equal(1, stats.LastTransformedEtags[EtlItemType.Document]);
        Assert.Equal(0, stats.LastLoadedEtag);

        Assert.Equal(1, stats.NumberOfExtractedItems[EtlItemType.Document]);
        Assert.Equal(1, stats.NumberOfTransformedItems[EtlItemType.Document]);
        Assert.Equal(0, stats.NumberOfLoadedItems);

        Assert.False(stats.SuccessfullyLoaded);

        // assert that next ETL batch starts from 0 etag
        var state = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
        var lastProcessedEtag = state.GetLastProcessedEtag(db.DbBase64Id, Server.ServerStore.NodeTag);
        Assert.Equal(0, lastProcessedEtag);

        // should not process this, we are still in rate-limit fallback cooldown 
        var etlDone = Etl.WaitForEtlToComplete(store);
        using (var session = store.OpenSession())
        {
            var post2 = new GenAiBasics.Post([
                new GenAiBasics.Comment("comment 1", "a"),
                new GenAiBasics.Comment("comment 2", "b"),
                new GenAiBasics.Comment("comment 3", "c"),
            ], "title", "body");

            session.Store(post2, docId2);
            session.SaveChanges();
        }

        Assert.False(await etlDone.WaitAsync(TimeSpan.FromSeconds(10)));

        using (var session = store.OpenSession())
        {
            var doc2 = session.Load<BlittableJsonReaderObject>(docId2);
            Assert.True(doc2.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.False(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject _));
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_LoadError_AuthFailure_ShouldOnlyTrackSuccess(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore();

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id); 
this.Comments[idx].IsSpam = $output.Blocked;";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}"
        };
        config.Identifier = "blog-post-spam-check";


        store.Maintenance.Send(new AddGenAiOperation(config));
        var db = await GetDatabase(store.Database);

        var etlProcess = db.EtlLoader.Processes.FirstOrDefault() as GenAiTask;
        Assert.NotNull(etlProcess);

        var chatCompletionClient = etlProcess.GetChatCompletionClient();
        var enteredOnce = 0;
        var blockEtlMre = new AsyncManualResetEvent();

        chatCompletionClient.ForTestingPurposesOnly().SimulateFailureAsync = ctx =>
        {
            if (ctx.Contains("alex"))
            {
                if (Interlocked.CompareExchange(ref enteredOnce, 1, 0) == 0)
                    throw new UnsuccessfulAiRequestException("Unauthorized", HttpStatusCode.Unauthorized) { RequestId = "fake-request-id" };

                return blockEtlMre.WaitAsync(TimeSpan.FromSeconds(60));
            }

            return Task.CompletedTask;
        };

        const string docId = "posts/1";

        using (var session = store.OpenSession())
        {
            var post = new GenAiBasics.Post([
                new GenAiBasics.Comment("Legit question about indexes", "alex"),
                new GenAiBasics.Comment("BUY DOGE NOW", "crypto_shill")
            ], "Indexes", "How do I optimize map-reduce?");

            session.Store(post, docId);
            session.SaveChanges();
        }

        EtlErrorInfo error = null;
        var value = await WaitForValueAsync(async () =>
        {
            error = await Etl.TryGetLoadErrorAsync(store.Database, config);
            return error != null;
        }, true, timeout: 60_000);

        Assert.True(value);
        Assert.NotNull(error);
        Assert.Contains("Unauthorized", error.Error);

        using (var session = store.OpenSession())
        {
            var doc = session.Load<BlittableJsonReaderObject>(docId);
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(config.Identifier, out BlittableJsonReaderArray hashes));

            // Only one comment should have succeeded
            Assert.Equal(1, hashes.Length);

            // assert that the update phase was executed only for comment #2
            Assert.True(doc.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
            Assert.Equal(2, comments.Length);

            var comment1 = comments[0] as BlittableJsonReaderObject;
            var comment2 = comments[1] as BlittableJsonReaderObject;

            Assert.NotNull(comment1);
            Assert.NotNull(comment2);

            Assert.False(comment1.TryGet("IsSpam", out bool _));
            Assert.True(comment2.TryGet("IsSpam", out bool _));
        }

        blockEtlMre.Set();

        // assert that next ETL batch starts from 0 etag
        var state = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
        var lastProcessedEtag = state.GetLastProcessedEtag(db.DbBase64Id, Server.ServerStore.NodeTag);
        Assert.Equal(0, lastProcessedEtag);

        // assert that the comment with model failure is processed in the next ETL batch
        var etlDone = Etl.WaitForEtlToComplete(store);

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromSeconds(60)));

        using (var session = store.OpenSession())
        {
            var doc = session.Load<BlittableJsonReaderObject>(docId);
            Assert.True(doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(config.Identifier, out BlittableJsonReaderArray hashes));

            // now both contexts should have their hash in metadata
            Assert.Equal(2, hashes.Length);

            // assert that the update phase was executed for comment #2
            Assert.True(doc.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
            Assert.Equal(2, comments.Length);

            var comment1 = comments[0] as BlittableJsonReaderObject;
            var comment2 = comments[1] as BlittableJsonReaderObject;

            Assert.NotNull(comment1);
            Assert.NotNull(comment2);

            Assert.True(comment1.TryGet("IsSpam", out bool _));
            Assert.True(comment1.TryGet("IsSpam", out bool _));
        }

        // assert that next ETL batch will NOT start from 0 etag
        state = EtlProcess.GetProcessState(db, config.Name, config.Transforms[0].Name);
        lastProcessedEtag = state.GetLastProcessedEtag(db.DbBase64Id, Server.ServerStore.NodeTag);
        Assert.True(lastProcessedEtag > 0);
    }

    private async Task<string> AddDebugInfo(IDocumentStore store, GenAiConfiguration config)
    {
        var database = await GetDocumentDatabaseInstanceFor(store);
        var perfStats = Etl.GetEtlPerformanceStatsForDatabase(database);

        var sb = new StringBuilder();
        sb.AppendLine("ETL performance stats:").AppendLine(perfStats);

        var loadAlert = database.NotificationCenter.EtlNotifications.GetAlert<EtlErrorsDetails>(
            GenAiTask.GenAiTaskTag, $"{config.Name}/{config.Transforms.First().Name}", AlertReason.Etl_LoadError);

        if (loadAlert?.Details is EtlErrorsDetails details)
        {
            sb.AppendLine("Etl error details:")
                .AppendLine(string.Join(',', details.Errors.Select(e => e.Error)));
        }

        return sb.ToString();
    }
}
