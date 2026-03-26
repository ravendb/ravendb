using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Documents.ETL.Providers.AI.GenAi;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.Handlers.AI.Agents;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24973(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CanSetupTracingInGenAiTask(Options options, GenAiConfiguration config)
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

        // set up conversation tracing and expiration
        config.EnableTracing = true;
        config.ExpirationInSec = 60 * 60 * 24;

        store.Maintenance.Send(new AddGenAiOperation(config));

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

        using (var session = store.OpenSession())
        {
            var docs = session.Advanced.LoadStartingWith<Chat>($"{config.Identifier}/");
            Assert.Equal(3, docs.Length);

            foreach (var doc in docs)
            {
                Assert.Equal(4, doc.Messages.Count); // prompt, agent parameters, context, model response

                Assert.Equal(config.Prompt, doc.Messages[0].Content.ToString());
                Assert.Equal("system", doc.Messages[0].Role);

                var paramsMsg = doc.Messages[1].Content.ToString();
                Assert.Contains("AI Agent Parameters", paramsMsg);
                Assert.Equal("user", doc.Messages[1].Role);

                var ctx = doc.Messages[2].Content.ToString();
                Assert.True(comments.Any(c => ctx.Contains($"\"Text\":\"{c.Text}\",\"Author\":\"{c.Author}\",\"Id\":\"{c.Id}\"")));
                Assert.Equal("user", doc.Messages[1].Role);

                var response = doc.Messages[3].Content.ToString();
                Assert.True(response.Contains("\"Blocked\":"));
                Assert.True(response.Contains("\"Reason\":"));
                Assert.Equal("assistant", doc.Messages[3].Role);

                // verify document has expiration set up 
                var metadata = session.Advanced.GetMetadataFor(doc);
                Assert.True(metadata.TryGetValue(Raven.Client.Constants.Documents.Metadata.Expires, out var expires));
                Assert.False(string.IsNullOrEmpty(expires));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task TestGenAiShouldReturnConversationDocument(Options options, GenAiConfiguration config)
    {
        using (var store = GetDocumentStore(options))
        {
            const string id = "posts/1";

            List<GenAiBasics.Comment> comments = [
                new("This article really helped me understand how indexes work in RavenDB. Great write-up!", "sarah_j"),
                new("Learn how to make $5000/month from home! Visit click4cash.biz.example now!!!", "shady_marketer"),
                new("I tried this approach with IO_Uring in the past, but I run into problems with security around the IO systems and the CISO didn't let us deploy that to production. It is more mature at this point?", "dave")
            ];

            var post = new GenAiBasics.Post(
                comments,
                "Understanding Indexing in RavenDB",
                "Indexes in RavenDB are a powerful way to optimize query performance. This blog post walks through auto-indexes, static indexes, and best practices when designing queries that scale."
            );

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(post, id);
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

                foreach (var item in result.Results)
                {
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

                foreach (var item in result.Results)
                {
                    Assert.True(item.ModelOutput.Output.TryGet("Blocked", out bool _));
                    Assert.True(item.ModelOutput.Output.TryGet("Reason", out string _));

                    // assert conversation documents were created
                    var doc = item.ModelOutput.ConversationDocument;
                    Assert.NotNull(doc);

                    AssertDocument(doc, config, comments);
                }
            }
        }
    }

    private class Chat
    {
        public List<Message> Messages { get; set; }
    }

    private class Message
    {
        [JsonProperty("role")]
        public string Role { get; set; }

        [JsonProperty("content")]
        public JToken Content { get; set; }
    }

    private static void AssertDocument(BlittableJsonReaderObject document, GenAiConfiguration configuration, List<GenAiBasics.Comment> comments)
    {
        try
        {
            Assert.True(document.TryGet(nameof(ConversationDocument.Messages), out BlittableJsonReaderArray messages));
            Assert.Equal(4, messages.Length);

            // prompt message
            var msgAsObj = messages[0] as BlittableJsonReaderObject;
            Assert.NotNull(msgAsObj);
            Assert.True(msgAsObj.TryGet("content", out string content));
            Assert.Equal(configuration.Prompt, content);

            Assert.True(msgAsObj.TryGet("role", out string role));
            Assert.Equal("system", role);

            // agent parameters message
            msgAsObj = messages[1] as BlittableJsonReaderObject;
            Assert.NotNull(msgAsObj);
            Assert.True(msgAsObj.TryGet("content", out content));
            Assert.Contains("AI Agent Parameters", content);

            Assert.True(msgAsObj.TryGet("role", out role));
            Assert.Equal("user", role);

            // context object message
            msgAsObj = messages[2] as BlittableJsonReaderObject;
            Assert.NotNull(msgAsObj);
            Assert.True(msgAsObj.TryGet("content", out content));
            Assert.True(comments.Any(c => content.Contains($"\"Text\":\"{c.Text}\",\"Author\":\"{c.Author}\",\"Id\":\"{c.Id}\"")));

            Assert.True(msgAsObj.TryGet("role", out role));
            Assert.Equal("user", role);

            // model response message
            msgAsObj = messages[3] as BlittableJsonReaderObject;
            Assert.NotNull(msgAsObj);
            Assert.True(msgAsObj.TryGet("content", out BlittableJsonReaderObject contentObj));

            Assert.True(contentObj.TryGet("Blocked", out bool _));
            Assert.True(contentObj.TryGet("Reason", out string _));

            Assert.True(msgAsObj.TryGet("role", out role));
            Assert.Equal("assistant", role);
        }
        catch (Exception e)
        {
            var sb = new StringBuilder()
                .AppendLine("Conversation document assertion failed: unexpected structure or missing/invalid fields.")
                .AppendLine()
                .AppendLine("Document:")
                .AppendLine(document.ToString())
                .AppendLine("Failure:")
                .AppendLine(e.Message);
            
            Assert.Fail(sb.ToString());
        }
    }
}
