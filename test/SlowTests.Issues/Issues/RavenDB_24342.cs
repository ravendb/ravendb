using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_24342 : RavenTestBase
    {
        public RavenDB_24342(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.vLLM, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task PutInUpdateScriptShouldUseSourceDocumentIdNotNull(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            config.Prompt = "Check if the following blog post comment is spam or not";
            config.Collection = "Posts";
            config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam" });
            config.UpdateScript = @" const newDocument = { ""modelOutput"": $output.Blocked, ""@metadata"": { ""@collection"": ""someNewCollection""} };
put(id(this) +""/new/"", newDocument);";
            config.GenAiTransformation = new GenAiTransformation
            {
                Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
            };

            await store.Maintenance.SendAsync(new AddGenAiOperation(config));
            var etl = Etl.WaitForEtlToComplete(store);

            using (var session = store.OpenAsyncSession())
            {
                var p = new Post(
                    [
                        new Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                        new Comment(
                            "Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.",
                            "Oren Eini")
                    ],
                    "I, pencil",
                    "A B52 pencil...");
                await session.StoreAsync(p);
                await session.SaveChangesAsync();
            }

            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(30)));

            using var verify = store.OpenAsyncSession();
            var ids = (await verify.Query<Comment>(collectionName: "someNewCollection").Select(x => x.Id).ToListAsync());

            Assert.DoesNotContain(ids, id => id.StartsWith("null/new/"));
            Assert.Contains(ids, id => id.StartsWith("posts/1-A/new/"));
        }

        internal record Post(List<Comment> Comments, string Title, string Body);

        internal record Comment
        {
            public string Text { get; set; }
            public string Author { get; set; }
            public Comment() { }

            public Comment(string text, string author)
            {
                Text = text;
                Author = author;
            }

            public string Id { get; set; } = Guid.NewGuid().ToString();
        }
    }
}
