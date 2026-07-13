using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Channels;
using Raven.Quill.Infrastructure;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// RavenDB-26775 follow-up: per-app DBs get Expiration enabled (so the
/// <c>@expires</c> on minted <see cref="EmbedLink"/>s actually deletes them) and
/// Revisions configured on the EmbedLinks collection (PurgeOnDelete=false) so an
/// expired/deleted link leaves an audit trail. Covers
/// <see cref="AppDatabaseFeatures.ConfigureAsync"/>.
/// </summary>
public class AppDatabaseFeaturesTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task ConfigureAsync_enables_expiration_and_embedlinks_revisions()
    {
        var store = GetDocumentStore();

        await AppDatabaseFeatures.ConfigureAsync(store, store.Database, CancellationToken.None);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

        Assert.NotNull(record.Expiration);
        Assert.False(record.Expiration.Disabled);

        Assert.NotNull(record.Revisions);
        // Collection key is "EmbedLinks" (CLR type pluralized), NOT the lowercase
        // "embed-links/" id prefix.
        var collectionName = store.Conventions.GetCollectionName(typeof(EmbedLink));
        Assert.Equal("EmbedLinks", collectionName);
        var coll = record.Revisions.Collections[collectionName];
        Assert.False(coll.Disabled);
        Assert.False(coll.PurgeOnDelete);
        Assert.Equal(10L, coll.MinimumRevisionsToKeep);
        // No age floor: with one, RavenDB keeps every revision younger than it, so a
        // high-cap link would accumulate ~1 revision/turn. Keep-newest-10 bounds it.
        Assert.Null(coll.MinimumRevisionAgeToKeep);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task ConfigureAsync_deploys_conversation_metrics_index()
    {
        var store = GetDocumentStore();

        await AppDatabaseFeatures.ConfigureAsync(store, store.Database, CancellationToken.None);

        var indexNames = await store.Maintenance.SendAsync(new GetIndexNamesOperation(0, 50));
        Assert.Contains("Conversations/Metrics", indexNames);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task EmbedLink_updates_produce_revisions()
    {
        var store = GetDocumentStore();
        await AppDatabaseFeatures.ConfigureAsync(store, store.Database, CancellationToken.None);

        var id = EmbedLink.IdPrefix + Guid.NewGuid().ToString("N");
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new EmbedLink
            {
                Id = id,
                WidgetId = "wgt_x",
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
            // create + 2 updates = 3 revisions (under the keep-10 cap, none purged).
            Assert.True(revisions.Count > 1, $"expected >1 revision, got {revisions.Count}");
        }
    }
}
