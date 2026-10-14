using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi
{
    public class GenAiAnthropicLive(ITestOutputHelper output) : RavenTestBase(output)
    {
        private record BlogComment(string Id, string Text, string Author);

        private record BlogPost(string Title, List<BlogComment> Comments);

        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.Anthropic, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Claude_ClassifiesSpam_AndUpdateScriptAppliesStructuredOutput(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "You are a spam classifier for blog comments. Decide whether the given comment is spam.";
            config.Collection = "BlogPosts";
            config.Identifier = "claude-spam-check";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Short reason for the decision" });
            config.UpdateScript = @"
    const idx = this.Comments.findIndex(c => c.Id == $input.Id);
    if (idx >= 0 && $output.Blocked)
        this.Comments.splice(idx, 1);
    ";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = "for (const c of this.Comments) ai.genContext({ Id: c.Id, Text: c.Text });"
            };

            var etl = Etl.WaitForEtlToComplete(store);

            store.Maintenance.Send(new AddGenAiOperation(config));

            const string docId = "posts/1";
            using (var session = store.OpenSession())
            {
                session.Store(new BlogPost("Understanding RavenDB indexing", new List<BlogComment>
                {
                    new("spam", "FREE CRYPTO AIRDROP!!! Claim your $$$ now at scamcoin.fake — limited time, act quick!", "bot"),
                    new("legit", "Great write-up, this finally made map/reduce indexes click for me. Thanks!", "alex"),
                }), docId);
                session.SaveChanges();
            }

            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(90)), "GenAI ETL did not finish in time");

            using (var session = store.OpenSession())
            {
                var post = session.Load<BlogPost>(docId);
                Assert.NotNull(post);

                Assert.DoesNotContain(post.Comments, c => c.Id == "spam");
                Assert.Contains(post.Comments, c => c.Id == "legit");
            }
        }
    }
}
