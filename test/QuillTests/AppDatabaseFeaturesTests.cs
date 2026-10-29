using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Channels;
using Raven.Quill.Infrastructure;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AppDatabaseFeaturesTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task ConfigureAsync_enables_expiration_and_embedlinks_revisions()
    {
        var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.FindCollectionName = QuillConventions.FindCollectionName,
        });

        await AppDatabaseFeatures.ConfigureAsync(store, store.Database, CancellationToken.None);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

        Assert.NotNull(record.Expiration);
        Assert.False(record.Expiration.Disabled);

        Assert.NotNull(record.Revisions);
        // collection key is the @-prefixed system collection from QuillConventions, NOT the "embed-links/" id prefix
        var collectionName = store.Conventions.GetCollectionName(typeof(EmbedLink));
        Assert.Equal("@embed-links", collectionName);
        var coll = record.Revisions.Collections[collectionName];
        Assert.False(coll.Disabled);
        Assert.False(coll.PurgeOnDelete);
        Assert.Equal(10L, coll.MinimumRevisionsToKeep);
        // must stay null: an age floor keeps every younger revision, so keep-newest-10 alone bounds growth
        Assert.Null(coll.MinimumRevisionAgeToKeep);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task ConfigureAsync_deploys_the_preview_index_and_no_metrics_index()
    {
        var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.FindCollectionName = QuillConventions.FindCollectionName,
        });

        await AppDatabaseFeatures.ConfigureAsync(store, store.Database, CancellationToken.None);

        var indexNames = await store.Maintenance.SendAsync(new GetIndexNamesOperation(0, 50));
        Assert.Contains("ConversationPreviewsIndex", indexNames);
        Assert.DoesNotContain("Conversations/Metrics", indexNames);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task EmbedLink_updates_produce_revisions()
    {
        var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = s => s.Conventions.FindCollectionName = QuillConventions.FindCollectionName,
        });
        await AppDatabaseFeatures.ConfigureAsync(store, store.Database, CancellationToken.None);

        var id = EmbedLink.IdPrefix + Guid.NewGuid().ToString("N");
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new EmbedLink
            {
                Id = id,
                ChannelId = "x",
                AgentId = "a",
                ExpiresAt = DateTime.UtcNow.AddHours(1),
                MaxInvocations = 5,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        for (var turn = 0; turn < 2; turn++)
        {
            using var session = store.OpenAsyncSession();
            var link = await session.LoadAsync<EmbedLink>(id);
            link.InvocationCount++;
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var revisions = await session.Advanced.Revisions.GetForAsync<EmbedLink>(id);
            Assert.Equal(3, revisions.Count);
        }
    }
}
