using System.Net;
using System.Security.Cryptography;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Raven;
using Raven.Quill.WhatsApp;

namespace Raven.Quill.Endpoints;

public static class WhatsAppEndpoints
{
    public static void Map(WebApplication app)
    {
        // Bridge-to-web push. Mapped outside /api so nginx's api.* surface never routes
        // it, and excluded from OpenAPI; the loopback + shared-token guards below are
        // the actual authentication.
        app.MapPost("/internal/whatsapp/inbound", HandleInboundAsync)
            .AllowAnonymous()
            .ExcludeFromDescription();

        var group = app.MapGroup("/api/apps/{slug}").WithTags("whatsapp").RequireAuthorization();

        group.MapGet("/channels/{channelId}/whatsapp/pairing", GetPairingAsync)
            .WithName("whatsapp.pairing")
            .WithDescription(
                "Live pairing status for a WhatsApp Personal channel. While pairing, qr carries " +
                "the current (rotating) linked-device payload for the dashboard to render; polling " +
                "this endpoint always returns the freshest code. Also self-heals: if the bridge " +
                "lost the session (e.g. it restarted before the phone was linked), a new one is started.")
            .Produces<WhatsAppPairingResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);

        group.MapPost("/channels/{channelId}/whatsapp/pairing/restart", RestartPairingAsync)
            .WithName("whatsapp.pairingRestart")
            .WithDescription("Restarts pairing with a fresh QR code; a logged-out session has its credentials wiped first.")
            .Produces<WhatsAppPairingResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);

        group.MapGet("/whatsapp/health", GetHealthAsync)
            .WithName("whatsapp.health")
            .WithDescription(
                "Per-channel session state for the app's WhatsApp Personal channels, proxied from " +
                "the bridge. State is null when the bridge has no session or is unreachable.")
            .Produces<WhatsAppChannelHealthResponse[]>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetPairingAsync(
        string slug,
        string channelId,
        IDocumentStore store,
        IWhatsAppBridgeClient bridge,
        ILogger<WhatsAppLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);
        if (channel is not { Type: ChannelType.WhatsAppPersonal })
            return Results.NotFound(new ApiErrorResponse($"no WhatsApp channel '{channelId}' in app '{slug}'"));

        try
        {
            var status = await bridge.GetSessionStatusAsync(app.Database, channelId, ct);
            if (status is null)
            {
                // the bridge lost the session (restart before the phone was linked); recreate it
                await bridge.StartSessionAsync(app.Database, channelId, ct);
                status = await bridge.GetSessionStatusAsync(app.Database, channelId, ct)
                         ?? new WhatsAppSessionStatus(WhatsAppSessionState.Starting, null, null, null, null);
            }

            await PersistLinkStateAsync(session, channel, status, ct);
            return Results.Ok(ToPairingResponse(status));
        }
        catch (WhatsAppBridgeException e)
        {
            logger.LogWarning("WhatsApp pairing status unavailable for channel {ChannelId}: {Error}", channelId, e.Message);
            return BridgeUnavailable();
        }
    }

    private static async Task<IResult> RestartPairingAsync(
        string slug,
        string channelId,
        IDocumentStore store,
        IWhatsAppBridgeClient bridge,
        ILogger<WhatsAppLogger> logger,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);
        if (channel is not { Type: ChannelType.WhatsAppPersonal })
            return Results.NotFound(new ApiErrorResponse($"no WhatsApp channel '{channelId}' in app '{slug}'"));

        try
        {
            await bridge.RestartSessionAsync(app.Database, channelId, ct);
            var status = await bridge.GetSessionStatusAsync(app.Database, channelId, ct)
                         ?? new WhatsAppSessionStatus(WhatsAppSessionState.Starting, null, null, null, null);

            await PersistLinkStateAsync(session, channel, status, ct);

            logger.LogInformation(
                "WhatsApp pairing restarted slug={Slug} channelId={ChannelId}", app.Slug, channelId);

            return Results.Ok(ToPairingResponse(status));
        }
        catch (WhatsAppBridgeException e)
        {
            logger.LogWarning("WhatsApp pairing restart failed for channel {ChannelId}: {Error}", channelId, e.Message);
            return BridgeUnavailable();
        }
    }

    private static async Task<IResult> GetHealthAsync(
        string slug,
        IDocumentStore store,
        IWhatsAppBridgeClient bridge,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        List<Channel> channels;
        using (var session = store.OpenAsyncSession(app.Database))
            channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

        var items = new List<WhatsAppChannelHealthResponse>();
        foreach (var channel in channels.Where(c => c.Type == ChannelType.WhatsAppPersonal).OrderByDescending(c => c.CreatedAt))
        {
            var channelId = Channel.StripIdPrefix(channel.Id);

            WhatsAppSessionStatus? status = null;
            string? lastError = null;
            try
            {
                status = await bridge.GetSessionStatusAsync(app.Database, channelId, ct);
                lastError = status?.LastError;
            }
            catch (WhatsAppBridgeException)
            {
                lastError = "whatsapp bridge is unavailable";
            }

            items.Add(new WhatsAppChannelHealthResponse(
                channelId,
                channel.WhatsApp?.PhoneNumber ?? status?.PhoneNumber,
                channel.Enabled,
                status?.State,
                lastError));
        }

        return Results.Ok(items.ToArray());
    }

    /// Mirrors the bridge-observed link state onto the channel doc so the channels
    /// list can show the phone number without a bridge round-trip per row.
    private static async Task PersistLinkStateAsync(
        IAsyncDocumentSession session, Channel channel, WhatsAppSessionStatus status, CancellationToken ct)
    {
        var settings = channel.WhatsApp ??= new WhatsAppSettings();

        if (status.State == WhatsAppSessionState.Connected &&
            string.IsNullOrEmpty(status.PhoneNumber) == false &&
            settings.PhoneNumber != status.PhoneNumber)
        {
            settings.PhoneNumber = status.PhoneNumber;
            settings.LinkedAt = DateTime.UtcNow;
            await session.SaveChangesAsync(ct);
        }
        else if (status.State == WhatsAppSessionState.LoggedOut && settings.PhoneNumber is not null)
        {
            settings.PhoneNumber = null;
            settings.LinkedAt = null;
            await session.SaveChangesAsync(ct);
        }
    }

    private static WhatsAppPairingResponse ToPairingResponse(WhatsAppSessionStatus status) =>
        new(status.State, status.Qr, status.QrExpiresAt, status.PhoneNumber, status.LastError);

    private static IResult BridgeUnavailable() =>
        Results.Json(
            new ApiErrorResponse("whatsapp bridge is unavailable", Code: "bridge_unavailable"),
            statusCode: StatusCodes.Status502BadGateway);

    private static async Task<IResult> HandleInboundAsync(
        HttpContext ctx,
        IDocumentStore store,
        IWhatsAppBridgeSecret secret,
        WhatsAppInboundProcessor processor,
        ILogger<WhatsAppLogger> logger,
        CancellationToken ct)
    {
        // UseForwardedHeaders rewrites nginx-relayed requests to the real client IP, so
        // only the bridge's direct loopback connection (or an in-process test server,
        // which has no remote address) passes this check. 404 keeps the route unadvertised.
        if (IsLoopback(ctx.Connection.RemoteIpAddress) == false)
            return Results.NotFound();

        var token = await secret.GetAsync(ct);
        var provided = ctx.Request.Headers["X-Quill-Bridge-Token"].ToString();
        if (token is null || TokensMatch(provided, token) == false)
            return Results.Unauthorized();

        // bound manually so the guards above run before any request parsing
        WhatsAppInboundRequest? body;
        try
        {
            body = await ctx.Request.ReadFromJsonAsync<WhatsAppInboundRequest>(ct);
        }
        catch (System.Text.Json.JsonException)
        {
            body = null;
        }

        if (body is null ||
            string.IsNullOrWhiteSpace(body.Database) ||
            string.IsNullOrWhiteSpace(body.ChannelId) ||
            string.IsNullOrWhiteSpace(body.Sender) ||
            body.Kind is not ("text" or "unsupported"))
        {
            return Results.BadRequest(new ApiErrorResponse("database, channelId, sender and a known kind are required"));
        }

        // Unroutable messages are dropped with a 2xx: the bridge retries non-2xx, and
        // a message for a deleted/disabled channel will never become routable.
        Channel? channel;
        try
        {
            using var session = store.OpenAsyncSession(body.Database);
            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + body.ChannelId, ct);
        }
        catch (DatabaseDoesNotExistException)
        {
            logger.LogWarning("Dropped inbound WhatsApp message for unknown database {Database}", body.Database);
            return Results.Accepted();
        }

        if (channel is not { Type: ChannelType.WhatsAppPersonal, Enabled: true })
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug(
                    "Dropped inbound WhatsApp message for channel {ChannelId} (missing, wrong type, or disabled)",
                    body.ChannelId);
            }

            return Results.Accepted();
        }

        processor.Enqueue(body.Database, channel, body.Sender, body.Kind, body.Text);
        return Results.Accepted();
    }

    internal static bool IsLoopback(IPAddress? remoteIp) =>
        remoteIp is null || IPAddress.IsLoopback(remoteIp);

    private static bool TokensMatch(string provided, string expected) =>
        CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(provided)),
            SHA256.HashData(Encoding.UTF8.GetBytes(expected)));

    internal sealed class WhatsAppLogger;
}
