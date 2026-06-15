using FastTests;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Infrastructure;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// RavenDB-26775 follow-up: per-app DBs get Expiration enabled (so the
/// <c>@expires</c> on minted <see cref="EmbedLink"/>s actually deletes them) and
/// Revisions configured on the EmbedLinks collection (PurgeOnDelete=false) so an
/// expired/deleted link leaves an audit trail. Covers
/// <see cref="AppDatabaseFeatures.ConfigureAsync"/>.
/// </summary>
public class AppDatabaseFeaturesTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
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
        Assert.Equal(TimeSpan.FromDays(90), coll.MinimumRevisionAgeToKeep);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
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
