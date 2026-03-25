using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Server.Utils;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.GenAi.Issues;

public class RavenDB_24186(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Ai)]
    [RavenGenAiData(IntegrationType = RavenAiIntegration.OpenAi, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenAi_ShouldReflectContextDeletionsInMetadataHashes(Options options, GenAiConfiguration config)
    {
        using var store = GetDocumentStore(options);
        store.Maintenance.Send(new PutConnectionStringOperation<AiConnectionString>(config.Connection));

        var etl = Etl.WaitForEtlToComplete(store);

        var taskName = config.Name.ToLower().Replace("_", "-");
        config.Prompt = "Check if the following blog post comment is spam or not";
        config.Collection = "Posts";
        config.SampleObject = JsonConvert.SerializeObject(new { Blocked = true, Reason = "Concise reason for why this comment was marked as spam or ham" });
        config.UpdateScript = "// no-op for this test";
        config.GenAiTransformation = new GenAiTransformation
        {
            Script = @"
for(const comment of this.Comments)
{
    ai.genContext({Text: comment.Text, Author: comment.Author, Id: comment.Id});
}
"
        };

        store.Maintenance.Send(new AddGenAiOperation(config));
        
        const string docId = "posts/1";

        var post = new GenAiBasics.Post([
            new GenAiBasics.Comment("Free crypto airdrop! Sign up now at scamcoin.fake", "evil bot") { Id = "comments/1" },
            new GenAiBasics.Comment("Great article. Helped me understand indexing in RavenDB.", "alex") { Id = "comments/2" },
            new GenAiBasics.Comment("Surefire investment property in caiman islands, win $$$$ for sure, qucik!", "homepage") { Id = "comments/3" }
        ], "Understanding RavenDB Indexing", "Indexes in RavenDB are powerful...");

        using (var session = store.OpenSession())
        {
            session.Store(post, docId);
            session.SaveChanges();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(60)));

        var db = await GetDatabase(store.Database);
        long etag = 0;

        // First run: hashes should match all extracted contexts (3 comments)
        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashes));
            Assert.NotNull(hashes);

            // one per comment
            Assert.Equal(3, hashes.Length);

            etag = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(postDoc), db.DbBase64Id);
        }

        // Second run: delete one context source (remove a comment).
        // All remaining contexts are cached (their hashes already exist), so no model call is expected.
        // Still, @gen-ai-hashes should be updated to remove the stale hash of the deleted comment.
        etl = Etl.WaitForEtlToComplete(store, (s, statistics) => statistics.LastProcessedEtag > etag);

        using (var session = store.OpenSession())
        {
            post = session.Load<GenAiBasics.Post>(docId);

            // remove the middle one
            var removed = post.Comments.Single(c => c.Id == "comments/2");
            post.Comments.Remove(removed);

            session.SaveChanges();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(60)));

        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.True(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection));
            Assert.True(hashesSection.TryGet(taskName, out BlittableJsonReaderArray hashes));
            Assert.NotNull(hashes);

            Assert.Equal(2, hashes.Length);

            etag = ChangeVectorUtils.GetEtagById(session.Advanced.GetChangeVectorFor(postDoc), db.DbBase64Id);
        }

        // Remove both of the remaining comments, which are cached.
        // No model call expected, but @gen-ai-hashes should be cleared entirely since there are no more contexts.
        etl = Etl.WaitForEtlToComplete(store, (s, statistics) => statistics.LastProcessedEtag > etag);

        using (var session = store.OpenSession())
        {
            post = session.Load<GenAiBasics.Post>(docId);

            // remove all remaining comments
            post.Comments.Clear();
            session.SaveChanges();
        }

        Assert.True(await etl.WaitAsync(TimeSpan.FromSeconds(60)));

        // Verify that @gen-ai-hashes is removed entirely
        using (var session = store.OpenAsyncSession())
        {
            var postDoc = await session.LoadAsync<BlittableJsonReaderObject>(docId);
            Assert.NotNull(postDoc);

            Assert.True(postDoc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata));
            Assert.False(metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out object _));
        }
    }

}
