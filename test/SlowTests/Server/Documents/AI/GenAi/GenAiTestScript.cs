using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi;

public class GenAiTestScript : RavenTestBase
{
    public GenAiTestScript(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanTestGenAiScript(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
";
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}"

                };

                var testGenAiScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = config,
                    TestStage = TestStage.CreateContextObjects
                };

                // first stage - test context objects creation
                var result = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(result);
                Assert.Equal(3, result.Results.Count);

                Assert.NotNull(result.InputDocument);
                Assert.True(result.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                foreach (var item in result.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }

                // second stage - test model call

                testGenAiScript.Input = result.Results;
                testGenAiScript.TestStage = TestStage.SendToModel;
                result = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

                Assert.NotNull(result);
                Assert.Equal(3, result.Results.Count);

                var spamComments = 0;
                foreach (var item in result.Results)
                {
                    Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(item.ModelOutput.Output.TryGet("Reason", out string r));
                    Assert.NotNull(r);
                }

                // third stage - test update script

                testGenAiScript.Input = result.Results;
                testGenAiScript.TestStage = TestStage.ApplyUpdateScript;
                result = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

                Assert.NotNull(result);
                Assert.Equal(3, result.Results.Count);

                Assert.NotNull(result.OutputDocument);
                Assert.True(result.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

                var expected = 3 - spamComments;
                Assert.Equal(expected, comments.Length);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanReuseContextFromPreviousRun(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            var post = new GenAiBasics.Post(
                [
                    new("This is a legit comment", "user1"),
                    new("You won a FREE iPhone, click now!", "spammer")
                ],
                "Title", "Body");
            await session.StoreAsync(post, id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);

        using var contextPool = database.DocumentsStorage.ContextPool;
        database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);

        config.Collection = "Posts";
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments[idx].Spam = true;
}
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        var testGenAiScript = new TestGenAiScript
        {
            DocumentId = id,
            Configuration = config,
            TestStage = TestStage.CreateContextObjects
        };

        // test context objects creation
        var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        Assert.NotNull(firstRun);
        Assert.Equal(2, firstRun.Results.Count);

        Assert.NotNull(firstRun.InputDocument);
        Assert.True(firstRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
        Assert.Equal(2, comments.Length);

        foreach (var item in firstRun.Results)
        {
            Assert.NotNull(item.ContextOutput.Context);
            Assert.NotNull(item.ContextOutput.AiHash);
            Assert.False(item.ContextOutput.IsCached);
            Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
            Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
            Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));

            Assert.Null(item.ModelOutput);
        }

        using (var session = store.OpenAsyncSession())
        {
            // add another comment to the document 

            var p = await session.LoadAsync<GenAiBasics.Post>(id);
            p.Comments.Add(new GenAiBasics.Comment("3rd comment", "aviv"));

            await session.SaveChangesAsync();
        }

        // test sending to the model

        testGenAiScript.Input = firstRun.Results;
        testGenAiScript.TestStage = TestStage.SendToModel;

        var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

        // we should still have 2 context objects like before, not 3 (context objects were not created in the 2nd test run)
        Assert.NotNull(secondRun);
        Assert.Equal(2, secondRun.Results.Count);

        foreach (var item in secondRun.Results)
        {
            Assert.NotNull(item.ModelOutput?.Output);
            Assert.NotNull(item.ModelOutput?.Usage);

            Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
            Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));

            Assert.NotNull(item.ContextOutput.AiHash);
            Assert.False(item.ContextOutput.IsCached);
        }
    }
    
    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanReuseModelOutputFromPreviousRun(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("spam message $$$", "bot"),
                    new GenAiBasics.Comment("normal comment", "real_user")]
                , "Spam Check", "Some content"), id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            config.Collection = "Posts";
            config.Prompt = "Check if the following blog post comment is spam or not";
            config.SampleObject = JsonConvert.SerializeObject(
                new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
            config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments[idx].Spam = true;
}
";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            };

            var testGenAiScript = new TestGenAiScript
            {
                DocumentId = id,
                Configuration = config
            };

            // first, test creating context objects
            var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
            Assert.NotNull(firstRun);
            foreach (var item in firstRun.Results)
            {
                Assert.NotNull(item.ContextOutput?.Context);
                Assert.Null(item.ModelOutput?.Output);
            }

            Assert.Null(firstRun.OutputDocument);

            testGenAiScript.Input = firstRun.Results;
            testGenAiScript.TestStage = TestStage.SendToModel;

            var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
            Assert.NotNull(secondRun);
            foreach (var item in secondRun.Results)
            {
                Assert.NotNull(item.ContextOutput?.Context);
                Assert.NotNull(item.ModelOutput?.Output);

                Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
                Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
            }

            // intentionally change the schema of the model output - in order to verify that the 3rd test run skips the model call
            testGenAiScript.TestStage = TestStage.ApplyUpdateScript;
            testGenAiScript.Input = secondRun.Results;
            testGenAiScript.Configuration.JsonSchema = null;
            testGenAiScript.Configuration.SampleObject = JsonConvert.SerializeObject(new
            {
                IsSpam = true,
                Explanation = "Concise reason for why this comment was marked as spam or harmful"
            });

            var thirdRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
            Assert.NotNull(thirdRun);

            foreach (var item in thirdRun.Results)
            {
                Assert.NotNull(item.ContextOutput?.Context);
                Assert.NotNull(item.ModelOutput?.Output);

                // model output should remain the same as in previous run

                Assert.False(item.ModelOutput.Output.TryGet("IsSpam", out bool _));
                Assert.False(item.ModelOutput.Output.TryGet("Explanation", out string _));

                Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
                Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
            }

            Assert.NotNull(thirdRun.OutputDocument);
        }


    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanModifyUpdateScript(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("spam message $$$", "bot"),
                    new GenAiBasics.Comment("normal comment", "real_user")]
                , "Spam Check", "Some content"), id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            config.Collection = "Posts";
            config.Prompt = "Check if the following blog post comment is spam or not";
            config.SampleObject = JsonConvert.SerializeObject(
                new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
            config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Spam = $output.Blocked;
";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            };

            var testGenAiScript = new TestGenAiScript
            {
                DocumentId = id,
                Configuration = config,
                TestStage = TestStage.CreateContextObjects
            };

            var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(firstRun);
            Assert.Equal(2, firstRun.Results.Count);
            testGenAiScript.Input = firstRun.Results;
            testGenAiScript.TestStage = TestStage.SendToModel;

            var second2Run = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(second2Run);
            Assert.Equal(2, second2Run.Results.Count);
            testGenAiScript.Input = second2Run.Results;
            testGenAiScript.TestStage = TestStage.ApplyUpdateScript;

            var thirdRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(thirdRun);
            Assert.Equal(2, thirdRun.Results.Count);
            Assert.NotNull(thirdRun.OutputDocument);

            Assert.True(thirdRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
            Assert.Equal(2, comments.Length);
            foreach (var item in comments)
            {
                var comment = item as BlittableJsonReaderObject;
                Assert.NotNull(comment);

                Assert.True(comment.TryGet("Spam", out bool b));
            }

            // change the update script and run again (just the update phase, skip context objects creation and model call)

            testGenAiScript.Input = thirdRun.Results;

            testGenAiScript.Configuration.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Reason = $output.Reason;
";

            var finalRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(finalRun);
            Assert.Equal(2, finalRun.Results.Count);
            Assert.NotNull(finalRun.OutputDocument);

            Assert.True(finalRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out comments));
            Assert.Equal(2, comments.Length);
            foreach (var item in comments)
            {
                var comment = item as BlittableJsonReaderObject;
                Assert.NotNull(comment);

                Assert.False(comment.TryGet("Spam", out bool b));
                Assert.True(comment.TryGet("Reason", out string r));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanModifyPromptAndSchema(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("Thanks for this article! I’ve been struggling with slow queries in RavenDB and your explanation of static vs. auto indexes really helped clarify the differences.", "tech_reader"),
                new GenAiBasics.Comment("🚨 HOT DEAL! Make $10,000 a week working from home. Limited time offer!!!", "spammer101"),
                new GenAiBasics.Comment("I found the indexing section insightful, especially the part about fanout indexes. I implemented one and saw query time drop by 80%.", "dev_jenny"),
                new GenAiBasics.Comment("Check out my blog for crazy RavenDB hacks and tricks they don’t want you to know 😈 raven-haxx.biz", "seo_bot")
                ],
                "Advanced RavenDB Indexing Strategies",
                "A deep dive into index design, fanout indexing, and performance tuning in RavenDB."
            ), id);

            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            config.Collection = "Posts";
            config.Prompt = "Check if the following blog post comment is spam or not";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
            config.UpdateScript = @"
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Spam = $output.Blocked;
";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            };

            var testGenAiScript = new TestGenAiScript
            {
                DocumentId = id,
                Configuration = config,
                TestStage = TestStage.CreateContextObjects
            };

            var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
            Assert.NotNull(firstRun);
            Assert.Equal(4, firstRun.Results.Count);

            foreach (var item in firstRun.Results)
            {
                Assert.NotNull(item.ContextOutput?.Context);
                Assert.Null(item.ModelOutput?.Output);
            }

            testGenAiScript.TestStage = TestStage.SendToModel;
            testGenAiScript.Input = firstRun.Results;
            var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(secondRun);
            Assert.Equal(4, secondRun.Results.Count);

            foreach (var item in secondRun.Results)
            {
                Assert.NotNull(item.ContextOutput?.Context);
                Assert.NotNull(item.ModelOutput?.Output);
                Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
                Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));
            }

            // now modify prompt + schema
            testGenAiScript.Input = secondRun.Results;

            testGenAiScript.Configuration.Prompt = @"
Check if the following blog post comment is legit or not (spam/harmful/bot). 
Provide an explanation, confidence level (0.0–1.0), and summarize the comment in one sentence.";

            testGenAiScript.Configuration.JsonSchema = null;
            testGenAiScript.Configuration.SampleObject = JsonConvert.SerializeObject(new
            {
                LegitComment = true,
                Explanation = "Concise reason for why this comment is legit",
                ConfidenceLevel = 0.95,
                Summary = "Summary of the comment's content"
            });

            var thirdRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
            Assert.NotNull(thirdRun);
            Assert.Equal(4, thirdRun.Results.Count);

            foreach (var item in thirdRun.Results)
            {
                var output = item.ModelOutput?.Output;
                Assert.NotNull(output);

                Assert.True(output.TryGet("LegitComment", out bool legit));
                Assert.True(output.TryGet("Explanation", out string explanation));
                Assert.True(output.TryGet("ConfidenceLevel", out double confidence));
                Assert.True(output.TryGet("Summary", out string summary));

                Assert.NotNull(explanation);
                Assert.InRange(confidence, 0.0, 1.0);
                Assert.NotNull(summary);

                Assert.False(item.ModelOutput.Output.TryGet("Blocked", out bool _));
                Assert.False(item.ModelOutput.Output.TryGet("Reason", out string _));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanEditDocumentAndTestAgain(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);
                await session.SaveChangesAsync();
            }

            var database = await GetDocumentDatabaseInstanceFor(store);
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                };

                var testGenAiScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = config,
                    TestStage = TestStage.CreateContextObjects
                };

                // first stage - test context objects creation
                var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(firstRun);
                Assert.Equal(3, firstRun.Results.Count);

                Assert.NotNull(firstRun.InputDocument);
                Assert.True(firstRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                foreach (var item in firstRun.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }

                // modify the document and test the context objects creation again
                var newComment = new GenAiBasics.Comment("Amazing post! By the way, I just made $7,000 last week using this weird AI trading bot — check it out at easyprofits-now.example!", "unreal");

                using (var session = store.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<GenAiBasics.Post>(id);
                    doc.Comments.Add(newComment);

                    var bjro = store.Conventions.Serialization.DefaultConverter.ToBlittable(doc, context);
                    testGenAiScript.Document = bjro;
                }

                var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(secondRun);
                Assert.Equal(4, secondRun.Results.Count);

                foreach (var item in secondRun.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanEditContextAndTestAgain(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                };

                var testGenAiScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = config,
                    TestStage = TestStage.CreateContextObjects
                };

                // first stage - test context objects creation
                var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(firstRun);
                Assert.Equal(3, firstRun.Results.Count);

                Assert.NotNull(firstRun.InputDocument);
                Assert.True(firstRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                foreach (var item in firstRun.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }

                // modify the context objects and test the second stage (model call)

                testGenAiScript.TestStage = TestStage.SendToModel;
                testGenAiScript.Input = firstRun.Results;

                var djv = new DynamicJsonValue
                {
                    ["Text"] = "Amazing post! By the way, I just made $7,000 last week using this weird AI trading bot — check it out at easyprofits-now.example!",
                    ["Author"] = "shady_author",
                    ["Id"] = "12345",
                };
                var contextObject = context.ReadObject(djv, id);

                testGenAiScript.Input.Add(new GenAiResultItem
                {
                    DocId = id,
                    ContextOutput = new ContextOutput
                    {
                        Context = contextObject, 
                        AiHash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(contextObject.ToString().AsSpan()))
                    }
                });

                var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(secondRun);
                Assert.Equal(4, secondRun.Results.Count);

                foreach (var item in secondRun.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));

                    Assert.NotNull(item.ModelOutput?.Output);
                }
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanTestGenAiScript_ViaEndpoint(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
";
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                };

                var testGenAiScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = config
                };

                // create context objects
                var bjro = store.Conventions.Serialization.DefaultConverter.ToBlittable(testGenAiScript, context);
                var cmd = new GenAiTestCmd(DocumentConventions.DefaultForServer, bjro);
                using var requestExecutor = store.GetRequestExecutor();
                await requestExecutor.ExecuteAsync(cmd, context);

                var result = cmd.Result;
                Assert.NotNull(result);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.Results), out BlittableJsonReaderArray resultItems));
                Assert.Equal(3, resultItems.Length);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.InputDocument), out BlittableJsonReaderObject input));
                Assert.NotNull(input);

                Assert.True(input.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                var dja = new DynamicJsonArray();
                testGenAiScript.Input = [];

                foreach (var item in resultItems)
                {
                    var asBlittable = (BlittableJsonReaderObject)item;
                    Assert.NotNull(asBlittable);
                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.ContextOutput), out BlittableJsonReaderObject contextOutput));
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.AiHash), out string hash));
                    Assert.NotNull(hash);
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.IsCached), out bool cached));
                    Assert.False(cached);

                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.Context), out BlittableJsonReaderObject ctxDoc));
                    Assert.True(ctxDoc.TryGet("Text", out string t));
                    Assert.True(ctxDoc.TryGet("Author", out string a));
                    Assert.True(ctxDoc.TryGet("Id", out string i));

                    // arrange the input for the next test-stage
                    dja.Add(new DynamicJsonValue
                    {
                        [nameof(GenAiResultItem.DocId)] = id,
                        [nameof(GenAiResultItem.ContextOutput)] = new DynamicJsonValue
                        {
                            [nameof(GenAiResultItem.ContextOutput.Context)] = new DynamicJsonValue
                            {
                                ["Text"] = t,
                                ["Author"] = a,
                                ["Id"] = i
                            },
                            [nameof(GenAiResultItem.ContextOutput.AiHash)] = hash
                        }
                    });
                }

                // send to model

                bjro.Modifications = new DynamicJsonValue(bjro)
                {
                    [nameof(TestGenAiScript.TestStage)] = TestStage.SendToModel,
                    [nameof(TestGenAiScript.Input)] = dja
                };
                using (var old = bjro)
                {
                    bjro = context.ReadObject(bjro, id);
                }

                cmd = new GenAiTestCmd(DocumentConventions.DefaultForServer, bjro);
                await requestExecutor.ExecuteAsync(cmd, context);

                result = cmd.Result;
                Assert.NotNull(result);
                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.Results), out resultItems));
                Assert.Equal(3, resultItems.Length);

                var spamComments = 0;
                dja = new DynamicJsonArray();
                foreach (var item in resultItems)
                {
                    var asBlittable = (BlittableJsonReaderObject)item;
                    Assert.NotNull(asBlittable);

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.ModelOutput), out BlittableJsonReaderObject modelOutput));
                    Assert.True(modelOutput.TryGet(nameof(GenAiResultItem.ModelOutput.Output), out BlittableJsonReaderObject output));

                    Assert.True(output.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(output.TryGet("Reason", out string r));
                    Assert.NotNull(r);

                    Assert.True(modelOutput.TryGet(nameof(GenAiResultItem.ModelOutput.Usage), out BlittableJsonReaderObject usage));
                    Assert.NotNull(usage);

                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.ContextOutput), out BlittableJsonReaderObject contextOutput));
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.AiHash), out string hash));
                    Assert.NotNull(hash);

                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.Context), out BlittableJsonReaderObject ctxDoc));
                    Assert.True(ctxDoc.TryGet("Id", out string i));

                    // arrange the input for the next test-stage

                    dja.Add(new DynamicJsonValue
                    {
                        [nameof(GenAiResultItem.DocId)] = id,
                        [nameof(GenAiResultItem.ContextOutput)] = new DynamicJsonValue
                        {
                            [nameof(GenAiResultItem.ContextOutput.AiHash)] = hash,
                            [nameof(GenAiResultItem.ContextOutput.Context)] = new DynamicJsonValue
                            {
                                ["Id"] = i
                            }
                        },
                        [nameof(GenAiResultItem.ModelOutput)] = new DynamicJsonValue
                        {
                            [nameof(GenAiResultItem.ModelOutput.Output)] = new DynamicJsonValue
                            {
                                ["Blocked"] = blocked,
                                ["Reason"] = r
                            }                               
                        },
                    });
                }

                // now test the update script
                bjro.Modifications = new DynamicJsonValue(bjro)
                {
                    [nameof(TestGenAiScript.TestStage)] = TestStage.ApplyUpdateScript,
                    [nameof(TestGenAiScript.Input)] = dja
                };
                using (var old = bjro)
                {
                    bjro = context.ReadObject(bjro, id);
                }

                cmd = new GenAiTestCmd(DocumentConventions.DefaultForServer, bjro);
                await requestExecutor.ExecuteAsync(cmd, context);

                result = cmd.Result;
                Assert.NotNull(result);
                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.Results), out resultItems));
                Assert.Equal(3, resultItems.Length);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.OutputDocument), out BlittableJsonReaderObject outputDoc));
                Assert.NotNull(outputDoc);
                Assert.True(outputDoc.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

                var expected = 3 - spamComments;
                Assert.Equal(expected, comments.Length);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanTestGenAiScript_ViaEndpoint_WithDocumentAsInput(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            using (var session = store.OpenAsyncSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );
                await session.StoreAsync(p, id);

                await session.SaveChangesAsync();
            }

            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
";
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                };

                var testGenAiScript = new TestGenAiScript
                {
                    DocumentId = id,
                    Configuration = config
                };

                var djv = new DynamicJsonValue();
                using (var session = store.OpenAsyncSession())
                {
                    // modify the document (without saving) and use it as input for the GenAI test

                    var p = await session.LoadAsync<GenAiBasics.Post>(id);
                    var c = new GenAiBasics.Comment(
                        "🚀 Boost your site's performance and get 10x traffic instantly! No dev work required. Click here: best-optimizer.fake", "growth_hacker_ai");

                    p.Comments.Add(c);

                    djv[nameof(GenAiBasics.Post.Title)] = p.Title;
                    djv[nameof(GenAiBasics.Post.Body)] = p.Body;

                    var dja = new DynamicJsonArray();
                    foreach (var comment in p.Comments)
                    {
                        dja.Add(new DynamicJsonValue
                        {
                            [nameof(GenAiBasics.Comment.Author)] = comment.Author,
                            [nameof(GenAiBasics.Comment.Text)] = comment.Text,
                            [nameof(GenAiBasics.Comment.Id)] = comment.Id
                        });
                    }

                    djv[nameof(GenAiBasics.Post.Comments)] = dja;
                }

                // create context objects
                var bjro = store.Conventions.Serialization.DefaultConverter.ToBlittable(testGenAiScript, context);
                bjro.Modifications = new DynamicJsonValue(bjro)
                {
                    [nameof(TestGenAiScript.Document)] = djv
                };

                bjro = context.ReadObject(bjro, id);

                var cmd = new GenAiTestCmd(DocumentConventions.DefaultForServer, bjro);
                using var requestExecutor = store.GetRequestExecutor();
                await requestExecutor.ExecuteAsync(cmd, context);

                var result = cmd.Result;
                Assert.NotNull(result);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.Results), out BlittableJsonReaderArray resultItems));
                Assert.Equal(4, resultItems.Length);

                Assert.True(result.TryGet(nameof(GenAiTestScriptResult.InputDocument), out BlittableJsonReaderObject input));
                Assert.NotNull(input);

                Assert.True(input.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(4, comments.Length);

                foreach (var item in resultItems)
                {
                    var asBlittable = (BlittableJsonReaderObject)item;
                    Assert.NotNull(asBlittable);
                    Assert.True(asBlittable.TryGet(nameof(GenAiResultItem.ContextOutput), out BlittableJsonReaderObject contextOutput));
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.AiHash), out string hash));
                    Assert.NotNull(hash);
                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.IsCached), out bool cached));
                    Assert.False(cached);

                    Assert.True(contextOutput.TryGet(nameof(GenAiResultItem.ContextOutput.Context), out BlittableJsonReaderObject ctxDoc));
                    Assert.True(ctxDoc.TryGet("Text", out string _));
                    Assert.True(ctxDoc.TryGet("Author", out string _));
                    Assert.True(ctxDoc.TryGet("Id", out string _));
                }
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task CanTestGenAi_WithFakeDocumentAndNoId(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                };
                config.UpdateScript = @"const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}
";

                var testGenAiScript = new TestGenAiScript
                {
                    Configuration = config,
                    TestStage = TestStage.CreateContextObjects
                };

                // create a document instance (without saving it) and pass it as input to TestScript

                var post = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                        new GenAiBasics.Comment("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                        new GenAiBasics.Comment("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
                    ],
                    "Understanding Indexing in RavenDB",
                    "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
                );

                testGenAiScript.Document = store.Conventions.Serialization.DefaultConverter.ToBlittable(post, context);

                // first stage - test context objects creation
                var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(firstRun);
                Assert.Equal(3, firstRun.Results.Count);

                Assert.NotNull(firstRun.InputDocument);
                Assert.True(firstRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(3, comments.Length);

                foreach (var item in firstRun.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }

                // modify the document and test the context objects creation again

                var newComment = new GenAiBasics.Comment("Amazing post! By the way, I just made $7,000 last week using this weird AI trading bot — check it out at easyprofits-now.example!", "unreal");
                post.Comments.Add(newComment);
                testGenAiScript.Document = store.Conventions.Serialization.DefaultConverter.ToBlittable(post, context);

                var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(secondRun);
                Assert.Equal(4, secondRun.Results.Count);

                Assert.NotNull(secondRun.InputDocument);
                Assert.True(secondRun.InputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out comments));
                Assert.Equal(4, comments.Length);

                foreach (var item in secondRun.Results)
                {
                    Assert.NotNull(item.ContextOutput.AiHash);
                    Assert.False(item.ContextOutput.IsCached);

                    Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                    Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
                }

                // test model call

                testGenAiScript.Input = secondRun.Results;
                testGenAiScript.TestStage = TestStage.SendToModel;

                var thirdRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(thirdRun);
                Assert.Equal(4, thirdRun.Results.Count);

                var spamComments = 0;
                foreach (var item in thirdRun.Results)
                {
                    Assert.NotNull(item.ContextOutput?.Context);
                    Assert.NotNull(item.ModelOutput?.Output);

                    Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool blocked));
                    if (blocked)
                        spamComments++;

                    Assert.True(item.ModelOutput.Output.TryGet("Reason", out string r));
                    Assert.NotNull(r);
                }

                // final stage - test update script

                testGenAiScript.Input = thirdRun.Results;
                testGenAiScript.TestStage = TestStage.ApplyUpdateScript;
                var finalRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

                Assert.NotNull(finalRun);
                Assert.Equal(4, finalRun.Results.Count);

                Assert.NotNull(finalRun.OutputDocument);
                Assert.True(finalRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

                var expected = 4 - spamComments;
                Assert.Equal(expected, comments.Length);
            }
        }
    }

    // todo: Fix test
    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Skip = "Failing test")] 
    public async Task TestGenAi_ShouldNotSendCachedItems(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("spam message $$$", "bot"),
                    new GenAiBasics.Comment("normal comment", "real_user")]
                , "Spam Check", "Some content"), id);
            await session.SaveChangesAsync();
        }

        var database = await GetDocumentDatabaseInstanceFor(store);

        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var testGenAiScript = new TestGenAiScript
            {
                DocumentId = id,
                Configuration = new()
                {
                    Name = "Check blog comments spam",
                    Connection = new AiConnectionString
                    {
                        Name = "ollama-local",
                        Identifier = "ollama-local",
                        OllamaSettings = new OllamaSettings
                        {
                            Uri = "http://127.0.0.1:11434/",
                            Model = "llama3.2:latest"
                        }
                    },
                    Collection = "Posts",
                    Prompt = "Check if the following blog post comment is spam or not",
                    SampleObject = JsonConvert.SerializeObject(
                    new
                    {
                        Blocked = true,
                        Reason = "Concise reason for why this comment was marked as spam or harmful"
                    }),
                    UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Spam = $output.Blocked;
",
                    GenAiTransformation = new GenAiTransformation
                    {
                        Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                    }
                },
                TestStage = TestStage.CreateContextObjects
            };

            var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(firstRun);
            Assert.Equal(2, firstRun.Results.Count);

            foreach (var item in firstRun.Results)
            {
                Assert.NotNull(item.ContextOutput.AiHash);
                Assert.False(item.ContextOutput.IsCached);

                Assert.True(item.ContextOutput.Context.TryGet("Text", out string _));
                Assert.True(item.ContextOutput.Context.TryGet("Author", out string _));
                Assert.True(item.ContextOutput.Context.TryGet("Id", out string _));
            }

            testGenAiScript.Input = firstRun.Results;
            testGenAiScript.TestStage = TestStage.SendToModel;

            var second2Run = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(second2Run);
            Assert.Equal(2, second2Run.Results.Count);
            testGenAiScript.Input = second2Run.Results;
            testGenAiScript.TestStage = TestStage.ApplyUpdateScript;

            var thirdRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(thirdRun);
            Assert.Equal(2, thirdRun.Results.Count);
            Assert.NotNull(thirdRun.OutputDocument);

            Assert.True(thirdRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
            Assert.Equal(2, comments.Length);
            foreach (var item in comments)
            {
                var comment = item as BlittableJsonReaderObject;
                Assert.NotNull(comment);

                Assert.True(comment.TryGet("Spam", out bool b));
            }

            // use the output document as an input for the test-mode
            // test context objects creation again - everything should be cached

            var outputDoc = thirdRun.OutputDocument;

            testGenAiScript.Input = thirdRun.Results;
            testGenAiScript.Document = outputDoc;
            testGenAiScript.TestStage = TestStage.CreateContextObjects;

            var forthRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(forthRun);
            Assert.Equal(2, forthRun.Results.Count);

            foreach (var item in forthRun.Results)
            {
                Assert.True(item.ContextOutput.IsCached); // Fail!
            }

            // now, add a new comment to the document and use it as input for the test-mode

            Assert.True(outputDoc.TryGet(nameof(GenAiBasics.Post.Comments), out comments));

            comments.Modifications = new DynamicJsonArray();
            const string newCommentText = "I'm a new comment";

            comments.Modifications.Add(new DynamicJsonValue
            {
                [nameof(GenAiBasics.Comment.Text)] = newCommentText,
                [nameof(GenAiBasics.Comment.Author)] = "aviv",
                [nameof(GenAiBasics.Comment.Id)] = "42"
            });

            outputDoc.Modifications = new DynamicJsonValue(outputDoc)
            {
                [nameof(GenAiBasics.Post.Comments)] = comments
            };

            using (var old = outputDoc)
            {
                outputDoc = context.ReadObject(outputDoc, id);
            }

            testGenAiScript.Input = forthRun.Results;
            testGenAiScript.Document = outputDoc;

            // test objects creation once again
            // the context object of the new comment should not be cached
            // all other context objects should be cached

            var finalRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(finalRun);
            Assert.Equal(3, finalRun.Results.Count);

            bool hasNewCommentInResult = false;

            foreach (var item in finalRun.Results)
            {
                Assert.True(item.ContextOutput.Context.TryGet("Text", out string text));

                if (text.Equals(newCommentText))
                {
                    hasNewCommentInResult = true;
                    Assert.False(item.ContextOutput.IsCached);
                    continue;
                }

                Assert.True(item.ContextOutput.IsCached);
            }

            Assert.True(hasNewCommentInResult);

        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false, Skip = "need to fix")]
    public async Task TestGenAi_ShouldTrackAiHashesInMetadata(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        const string id = "posts/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new GenAiBasics.Post([
                    new GenAiBasics.Comment("spam message $$$", "bot"),
                    new GenAiBasics.Comment("normal comment", "real_user"),
                    new GenAiBasics.Comment("harmful content", "evil bot")
                ]
                , "Spam Check", "Some content"), id);
            await session.SaveChangesAsync();
        }

        config.Collection = "Posts";
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.JsonSchema = OllamaChatCompletionClient.GetSchemaFor(JsonConvert.SerializeObject(new
        {
            Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful"
        }));
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
this.Comments[idx].Spam = $output.Blocked;
";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        var testGenAiScript = new TestGenAiScript { DocumentId = id, Configuration = config, TestStage = TestStage.CreateContextObjects };

        var database = await GetDocumentDatabaseInstanceFor(store);
        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(firstRun);
            Assert.Equal(3, firstRun.Results.Count);

            testGenAiScript.Input = firstRun.Results;
            testGenAiScript.TestStage = TestStage.SendToModel;

            var second2Run = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(second2Run);
            Assert.Equal(3, second2Run.Results.Count);
            testGenAiScript.Input = second2Run.Results;
            testGenAiScript.TestStage = TestStage.ApplyUpdateScript;

            var finalRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

            Assert.NotNull(finalRun);
            Assert.Equal(3, finalRun.Results.Count);
            Assert.NotNull(finalRun.OutputDocument);

            Assert.True(finalRun.OutputDocument.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(testGenAiScript.Configuration.Name, out BlittableJsonReaderArray hashesArr));

            var hashes = hashesArr.Select(x => x.ToString()).ToList();

            List<string> expectedHashes = new();
            var prompt = testGenAiScript.Configuration.Prompt;
            var schema = testGenAiScript.Configuration.JsonSchema;
            var update = testGenAiScript.Configuration.UpdateScript;

            foreach (var item in finalRun.Results)
            {
                var wrapped = new DynamicJsonValue { ["Context"] = item.ContextOutput.Context, ["Prompt"] = prompt, ["Schema"] = schema, ["Update"] = update };

                using var wrappedBlittable = context.ReadObject(wrapped, "hash");
                var hash = AttachmentsStorageHelper.CalculateHash(wrappedBlittable.AsSpan());
                expectedHashes.Add(hash);
            }

            Assert.Equal(expectedHashes.Count, hashes.Count);

            var expectedStr = string.Join(',', expectedHashes);
            Output.WriteLine("expected hashes: " + expectedStr);

            var actualStr = string.Join(',', hashes);

            Output.WriteLine("actual hashes: " + actualStr);


            foreach (var hash in expectedHashes)
            {
                Output.WriteLine("checking for " + hash + " in hashes");

                Assert.Contains(hash, hashes);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.Ollama, DatabaseMode = RavenDatabaseMode.Single, CheckCanConnect = false, NightlyBuildRequired = false)]
    public async Task ShouldStripMetadataPropertiesFromInputDocument(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            var database = await GetDocumentDatabaseInstanceFor(store);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                config.Collection = "Posts";
                config.Prompt = "Check if the following blog post comment is spam or not";
                config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                config.GenAiTransformation = new GenAiTransformation
                {
                    Script = @"
for (const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
                };
                config.UpdateScript =
@"const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if (idx < 0)
    return;
this.Comments[idx].IsSpam = $output.Blocked;
";

                var testGenAiScript = new TestGenAiScript
                {
                    Configuration = config,
                    TestStage = TestStage.CreateContextObjects
                };

                // create a sample document to test on
                var djv = new DynamicJsonValue
                {
                    [nameof(GenAiBasics.Post.Title)] = "Understanding Indexing in RavenDB",
                    [nameof(GenAiBasics.Post.Body)] = "Indexes in RavenDB are a powerful way to optimize query performance. " +
                                                      "This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale.",

                    [nameof(GenAiBasics.Post.Comments)] = new DynamicJsonArray
                    {
                        new DynamicJsonValue
                        {
                            [nameof(GenAiBasics.Comment.Author)] = "sarah_j",
                            [nameof(GenAiBasics.Comment.Text)] = "This article really helped me understand how indexes work in RavenDB. Great write-up!",
                            [nameof(GenAiBasics.Comment.Id)] = "1"

                        },
                        new DynamicJsonValue
                        {
                            [nameof(GenAiBasics.Comment.Author)] = "shady_marketer",
                            [nameof(GenAiBasics.Comment.Text)] = "Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!!",
                            [nameof(GenAiBasics.Comment.Id)] = "2"
                        }
                    },

                    // add metadata properties to the document
                    [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Id] = "posts/1-A",
                        [Constants.Documents.Metadata.Collection] = "Posts",
                        [Constants.Documents.Metadata.LastModified] = DateTime.UtcNow,
                        [Constants.Documents.Metadata.Flags] = new DynamicJsonArray { "HasCounters", "HasTimeSeries" },
                        [Constants.Documents.Metadata.IndexScore] = 123
                    }
                };

                testGenAiScript.Document = context.ReadObject(djv, "test-document");

                // first stage - test context objects creation
                var firstRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(firstRun);
                Assert.Equal(2, firstRun.Results.Count);

                // test model call
                testGenAiScript.Input = firstRun.Results;
                testGenAiScript.TestStage = TestStage.SendToModel;

                var secondRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;
                Assert.NotNull(secondRun);
                Assert.Equal(2, secondRun.Results.Count);

                foreach (var item in secondRun.Results)
                {
                    Assert.NotNull(item.ModelOutput?.Output);

                    Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
                    Assert.True(item.ModelOutput.Output.TryGet("Reason", out string r));
                    Assert.NotNull(r);
                }

                // final stage - test update script
                // should not throw on unfiltered metadata properties

                testGenAiScript.Input = secondRun.Results;
                testGenAiScript.TestStage = TestStage.ApplyUpdateScript;
                var finalRun = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

                Assert.NotNull(finalRun);
                Assert.Equal(2, finalRun.Results.Count);

                Assert.NotNull(finalRun.OutputDocument);
                Assert.True(finalRun.OutputDocument.TryGet(nameof(GenAiBasics.Post.Comments), out BlittableJsonReaderArray comments));
                Assert.Equal(2, comments.Length);

                foreach (var o in comments)
                {
                    var comment = o as BlittableJsonReaderObject;
                    Assert.NotNull(comment);

                    Assert.True(comment.TryGet("IsSpam", out bool _));
                }

                Assert.True(finalRun.OutputDocument.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
                Assert.False(metadata.TryGet(Constants.Documents.Metadata.Id, out object _));
                Assert.False(metadata.TryGet(Constants.Documents.Metadata.LastModified, out object _));
                Assert.False(metadata.TryGet(Constants.Documents.Metadata.IndexScore, out object _));
                Assert.False(metadata.TryGet(Constants.Documents.Metadata.Flags, out object _));

                Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject _));
            }
        }
    }

    private class GenAiTestCmd : RavenCommand<BlittableJsonReaderObject>
    {
        private readonly DocumentConventions _conventions;
        private readonly BlittableJsonReaderObject _testScript;
        public override bool IsReadRequest => true;

        public GenAiTestCmd(DocumentConventions conventions, BlittableJsonReaderObject testScript)
        {
            _conventions = conventions;
            _testScript = testScript;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/gen-ai/test";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _testScript).ConfigureAwait(false), _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = response;
        }
    }
}
