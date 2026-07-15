using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;

namespace Raven.Quill.Infrastructure;

internal static class AppDatabaseFeatures
{
    private const int MinimumRevisionsToKeep = 10;

    public static async Task ConfigureAsync(IDocumentStore store, string database, CancellationToken ct)
    {
        await EnableExpirationAsync(store, database, ct);

        await new ConversationMetricsIndex().ExecuteAsync(store, database: database, token: ct);

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
