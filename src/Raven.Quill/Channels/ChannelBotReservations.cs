using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;

namespace Raven.Quill.Channels;

internal interface IChannelBotReservation
{
    string Database { get; set; }

    string ChannelId { get; set; }
}

internal static class ChannelBotReservations
{
    internal readonly record struct Claim(bool Acquired, string? OwnerDatabase, string? ChangeVector);

    internal static async Task<Claim> TryClaimAsync<TReservation>(
        IDocumentStore store,
        string reservationId,
        string database,
        string channelId,
        Func<Channel?, bool> isLive,
        Func<IAsyncDocumentSession, TReservation, Task>? storeCompanions,
        CancellationToken ct)
        where TReservation : class, IChannelBotReservation, new()
    {
        using var configSession = store.OpenAsyncSession();

        var reservation = await configSession.LoadAsync<TReservation>(reservationId, ct);
        if (reservation is not null && reservation.Database == database && reservation.ChannelId == channelId)
        {
            if (storeCompanions is null)
                return new Claim(true, null, configSession.Advanced.GetChangeVectorFor(reservation));

            await configSession.StoreAsync(
                reservation, configSession.Advanced.GetChangeVectorFor(reservation), reservationId, ct);
        }
        else if (reservation is null)
        {
            reservation = new TReservation { Database = database, ChannelId = channelId };
            await configSession.StoreAsync(reservation, string.Empty, reservationId, ct);
        }
        else
        {
            if (await IsLiveAsync(store, reservation, isLive, ct))
                return new Claim(false, reservation.Database, null);

            // a reservation without a live matching channel is an orphan; reclaim it under its change vector
            reservation.Database = database;
            reservation.ChannelId = channelId;
            await configSession.StoreAsync(
                reservation, configSession.Advanced.GetChangeVectorFor(reservation), reservationId, ct);
        }

        if (storeCompanions is not null)
            await storeCompanions(configSession, reservation);

        try
        {
            await configSession.SaveChangesAsync(ct);
            return new Claim(true, null, configSession.Advanced.GetChangeVectorFor(reservation));
        }
        catch (ConcurrencyException)
        {
            return new Claim(false, null, null);
        }
    }

    internal static async Task<bool> TryConfirmAsync<TReservation>(
        IDocumentStore store,
        string reservationId,
        TReservation reservation,
        string changeVector,
        CancellationToken ct)
        where TReservation : class, IChannelBotReservation
    {
        try
        {
            using var configSession = store.OpenAsyncSession();
            await configSession.StoreAsync(reservation, changeVector, reservationId, ct);
            await configSession.SaveChangesAsync(ct);
            return true;
        }
        catch (ConcurrencyException)
        {
            return false;
        }
    }

    internal static async Task ReleaseAsync<TReservation>(
        IDocumentStore store,
        string reservationId,
        string database,
        string channelId,
        Func<IAsyncDocumentSession, Task>? releaseCompanions,
        ILogger logger)
        where TReservation : class, IChannelBotReservation
    {
        try
        {
            using var configSession = store.OpenAsyncSession();

            var reservation = await configSession.LoadAsync<TReservation>(reservationId);
            if (reservation is not null && reservation.Database == database && reservation.ChannelId == channelId)
                configSession.Delete(reservation);

            if (releaseCompanions is not null)
                await releaseCompanions(configSession);

            await configSession.SaveChangesAsync();
        }
        catch (Exception e)
        {
            // an unreleased reservation is an orphan the next claim reclaims
            logger.LogWarning("Bot reservation {ReservationId} was not released: {Error}", reservationId, e.Message);
        }
    }

    // live = the reserved channel still exists and still uses this bot
    private static async Task<bool> IsLiveAsync(
        IDocumentStore store, IChannelBotReservation reservation, Func<Channel?, bool> isLive, CancellationToken ct)
    {
        try
        {
            using var session = store.OpenAsyncSession(reservation.Database);
            return isLive(await session.LoadAsync<Channel>(reservation.ChannelId, ct));
        }
        catch (DatabaseDoesNotExistException)
        {
            return false;
        }
    }
}
