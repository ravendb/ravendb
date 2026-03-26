using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues
{
    public class RavenDB_25435(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenTheory(RavenTestCategory.Ai)]
        [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanCreateGenAiTask(Options options, GenAiConfiguration config)
        {
            using var store = GetDocumentStore(options);
            store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

            var identifier = ModifyGenAiConfig(config);
            config.ExpirationInSec = 60 * 60; // 1 hour
            var res = await store.Maintenance.SendAsync(new AddGenAiOperation(config));

            config.ExpirationInSec = 24 * 60 * 60; // 1 day
            await store.Maintenance.SendAsync(new UpdateGenAiOperation(res.TaskId, config));

            await TriggerGenAi(store);

            using (var session = store.OpenAsyncSession())
            {
                var docs = (await session.Advanced.LoadStartingWithAsync<Chat>($"{identifier}/")).ToList();
                Assert.Equal(2, docs.Count);
                foreach (var d in docs)
                {
                    var expiredIn = ExpiredIn(session, d);
                    Assert.True(expiredIn > TimeSpan.FromHours(12), $"Expired in: {expiredIn}");
                }
            }
        }

        private static TimeSpan ExpiredIn(IAsyncDocumentSession session, object d)
        {
            var metadata = session.Advanced.GetMetadataFor(d);
            Assert.True(metadata.TryGetValue(Constants.Documents.Metadata.Expires, out var expiration));
            var dateValue = DateTime.Parse(expiration, null, System.Globalization.DateTimeStyles.RoundtripKind);
            return dateValue.ToUniversalTime() - DateTime.Now.ToUniversalTime();
        }

        private static string ModifyGenAiConfig(GenAiConfiguration config)
        {
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
            config.Identifier = "posts-genai";
            config.EnableTracing = true;

            return config.Identifier;
        }

        private async Task TriggerGenAi(DocumentStore store)
        {
            var etl = Etl.WaitForEtlToComplete(store);
            using (var session = store.OpenSession())
            {
                var p = new GenAiBasics.Post(
                    [
                        new GenAiBasics.Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage"),
                        new GenAiBasics.Comment(
                            "Probably... That piece of code was written (and never looked at) in 2017, IIRC It wasn't a real issue (since it is cached) except for this particular scenario.",
                            "Oren Eini")
                    ],
                    "I, pencil",
                    "A B52 pencil...");
                session.Store(p);
                session.SaveChanges();
            }
            Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(30)));
        }

        internal record Comment(string Text, string Author)
        {
            public string Id { get; set; } = Guid.NewGuid().ToString();
        }

        internal record Post(List<Comment> Comments, string Title, string Body);

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
    }
}
