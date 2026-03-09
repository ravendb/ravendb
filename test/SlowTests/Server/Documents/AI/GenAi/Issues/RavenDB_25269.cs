using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_25269(ITestOutputHelper output) : RavenTestBase(output)
    {

        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task ShouldNotThrowWhenDocumentIsDeletedBetweenModelCallAndUpdate(Options options, GenAiConfiguration config)
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
                    config.Identifier = "RavenDB_25269";
                    config.Collection = "Posts";
                    config.Prompt = "Check if the following blog post comment is spam or not";
                    config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or harmful" });
                    config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);
this.Comments[idx].IsBlocked = $output.Blocked;
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

                    // second stage - test model call
                    testGenAiScript.Input = result.Results;
                    testGenAiScript.TestStage = TestStage.SendToModel;
                    result = GenAiTask.TestScript(testGenAiScript, database, database.ServerStore, context) as GenAiTestScriptResult;

                    Assert.NotNull(result);
                    Assert.Equal(3, result.Results.Count);

                    foreach (var item in result.Results)
                    {
                        Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool blocked));
                        Assert.True(item.ModelOutput.Output.TryGet("Reason", out string r));
                        Assert.NotNull(r);
                    }

                    // delete the document before testing the update script,
                    // to simulate a scenario where the document was deleted between model-call and update stages
                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(id);
                        await session.SaveChangesAsync();
                    }

                    // run update phase *manually* (in test mode the update phase goes through a different code path than regular GenAI flow)
                    var req = new PatchRequest(config.UpdateScript, PatchRequestType.GenAi);
                    var cmd = new GenAiBatchPatchCommand(result.Results, req, config.Identifier, RavenLogManager.Instance.GetLoggerForDatabase<RavenDB_25269>(database), new EtlProcessStatistics(), new GenAiStatsScope(new EtlRunStats()));

                    // should not throw NRE even if the document was deleted
                    await database.TxMerger.Enqueue(cmd);
                }
            }
        }
    }
}
