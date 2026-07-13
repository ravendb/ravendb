using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;

namespace Raven.Quill.Infrastructure;

/// <summary>
/// Configures the RavenDB features a per-app database needs for the embed-link
/// lifecycle (RavenDB-26775):
/// <list type="bullet">
///   <item><b>Expiration</b> — so the <c>@expires</c> metadata stamped on minted
///   <see cref="EmbedLink"/>s (and the config-DB <c>link-index</c> pointers)
///   actually deletes them once their TTL elapses, instead of accumulating.</item>
///   <item><b>Revisions on the EmbedLinks collection</b> — with
///   <c>PurgeOnDelete=false</c>, so when an expired link is deleted it leaves a
///   delete-revision: an audit trail of who was issued which link (bound params,
///   TTL, cap, agent) that survives the cleanup. <c>MinimumRevisionsToKeep=10</c>
///   with <b>no age floor</b> keeps only the newest ~10 revisions per link, so the
///   per-turn <c>InvocationCount++</c> churn is bounded regardless of the link's
///   invocation cap. (A <c>MinimumRevisionAgeToKeep</c> was deliberately NOT set:
///   RavenDB keeps every revision younger than the age floor, so with a floor above
///   the max TTL nothing would purge during a link's life and a high-cap link could
///   accumulate ~1 revision per turn — see <c>RevisionsStorage</c> purge logic.)</item>
/// </list>
/// </summary>
internal static class AppDatabaseFeatures
{
    private const int MinimumRevisionsToKeep = 10;

    /// <summary>Per-app DB: Expiration + Revisions on the EmbedLinks collection,
    /// plus the dashboard metric indexes.</summary>
    public static async Task ConfigureAsync(IDocumentStore store, string database, CancellationToken ct)
    {
        await EnableExpirationAsync(store, database, ct);

        // Dashboard stats read from these indexes; deploy at provision so the
        // stats endpoints never hit a missing-index error on a real app.
        // PutIndexesOperation (called by ExecuteAsync) compares definition hashes — no rebuild if unchanged.
        await new ConversationMetricsIndex().ExecuteAsync(store, database: database, token: ct);

        // The collection name is derived from the CLR type (EmbedLink -> "EmbedLinks"),
        // NOT the lowercase "embed-links/" doc-id prefix — keying the config on the
        // prefix would silently never match the collection.
        var embedLinks = store.Conventions.GetCollectionName(typeof(EmbedLink));

        var revisions = new RevisionsConfiguration
        {
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>
            {
                [embedLinks] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    PurgeOnDelete = false,
                    MinimumRevisionsToKeep = MinimumRevisionsToKeep,
                },
            },
        };

        await store.Maintenance.ForDatabase(database).SendAsync(new ConfigureRevisionsOperation(revisions), ct);
    }

    /// <summary>Enables document Expiration on <paramref name="database"/>. Idempotent —
    /// safe to call on every startup (config DB) or once per provision (app DB).</summary>
    public static async Task EnableExpirationAsync(IDocumentStore store, string database, CancellationToken ct)
    {
        await store.Maintenance.ForDatabase(database).SendAsync(
            new ConfigureExpirationOperation(new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 60,
            }), ct);
    }
}
