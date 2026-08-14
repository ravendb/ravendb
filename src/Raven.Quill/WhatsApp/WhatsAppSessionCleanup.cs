using Raven.Client.Documents;
using Raven.Quill.Channels;
using Raven.Quill.Raven;

namespace Raven.Quill.WhatsApp;

internal static class WhatsAppSessionCleanup
{
    /// Unlinks every WhatsApp session of an app before its database is dropped.
    /// Best effort: app deletion must not be blocked by a dead bridge; a session it
    /// could not wipe only lingers until the operator unlinks the phone manually.
    internal static async Task DeleteAllForDatabaseAsync(
        IDocumentStore store,
        IWhatsAppBridgeClient bridge,
        string database,
        ILogger logger,
        CancellationToken ct)
    {
        List<Channel> channels;
        using (var session = store.OpenAsyncSession(database))
            channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

        foreach (var channel in channels.Where(c => c.Type == ChannelType.WhatsAppPersonal))
        {
            var channelId = channel.ShortId;
            try
            {
                await bridge.DeleteSessionAsync(database, channelId, ct);
            }
            catch (WhatsAppBridgeException e)
            {
                logger.LogWarning(
                    "Could not unlink WhatsApp session for channel {ChannelId} while deleting app database {Database}: {Error}",
                    channelId, database, e.Message);
            }
        }
    }
}
