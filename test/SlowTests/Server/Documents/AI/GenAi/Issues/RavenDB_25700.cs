using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_25700(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnablingTracingAfterTaskUpdateShouldTakeEffect(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore();
        const string docId = "posts/1";

        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        config.Identifier = "gen-ai-spam-detection";
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";

        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = @"    
const idx = this.Comments.findIndex(c => c.Id == $input.Id);  
if($output.Blocked)
{
    this.Comments.splice(idx, 1); // remove
}";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        // conversation tracing is set to False initially
        config.EnableTracing = false;

        var genAiTask = store.Maintenance.Send(new AddGenAiOperation(config));

        var etlDone = Etl.WaitForEtlToComplete(store);

        List<GenAiBasics.Comment> comments = [
            new("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot"),
            new("Great article. Helped me understand indexing in RavenDB.", "alex"),
            new("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage")
        ];

        var post = new GenAiBasics.Post(comments, "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful...");

        using (var session = store.OpenSession())
        {
            session.Store(post, docId); 
            session.SaveChanges();
        }

        Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));


        var conversationDocsPrefix = $"{config.Identifier}/";

        using (var session = store.OpenSession())
        {
            var docs = session.Advanced.LoadStartingWith<object>(conversationDocsPrefix);

            // no conversation documents should be created when tracing is disabled
            Assert.Equal(0, docs.Length);
        }

        // update the task to enable tracing
        config.EnableTracing = true;
        store.Maintenance.Send(new UpdateGenAiOperation(genAiTask.TaskId, config));

        // add another post to trigger the ETL again
        using (var session = store.OpenSession())
        {
            var anotherPost = new GenAiBasics.Post(
                [
                    new("This is another spam comment, visit spammy.site.example now!", "spammer"),
                    new("Informative post, thanks for sharing!", "reader123")
                ],
                "RavenDB Performance Tuning",
                "In this post, we explore various techniques to optimize RavenDB performance..."
            );
            session.Store(anotherPost, "posts/2");
            session.SaveChanges();
        }

        // verify that conversation documents were created this time
        var docsCount = WaitForValue(() =>
        {
            using (var session = store.OpenSession())
            {
                var docs = session.Advanced.LoadStartingWith<object>(conversationDocsPrefix);
                return docs?.Length;
            }
        }, expectedVal: 2, timeout: 30_000);

        // two conversation documents should be created when tracing is enabled (one per comment)
        Assert.True(docsCount == 2, await Etl.GetEtlDebugInfo(store.Database));
    }
}
