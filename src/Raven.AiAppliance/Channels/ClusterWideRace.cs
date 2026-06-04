using Raven.Client.Documents;

namespace Raven.AiAppliance.Channels;

/// <summary>
/// After losing a <c>TransactionMode.ClusterWide</c> race, the winner's doc
/// is committed through Raft but can momentarily be invisible to a plain
/// read on this node. Retries the load (~500 ms budget) until it appears.
/// Shared by the <see cref="ChannelBinding"/> and
/// <see cref="ConversationBinding"/> race losers. Returns null when the doc
/// never becomes visible — callers own the throw (and must keep secrets,
/// e.g. conversation tokens, out of the message).
/// </summary>
internal static class ClusterWideRace
{
    internal static async Task<T?> LoadWinnerAsync<T>(IDocumentStore store, string database, string id, CancellationToken ct)
        where T : class
    {
        const int maxAttempts = 10;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            using (var session = store.OpenAsyncSession(database))
            {
                var doc = await session.LoadAsync<T>(id, ct);
                if (doc is not null)
                    return doc;
            }

            await Task.Delay(50, ct);
        }

        return null;
    }
}
