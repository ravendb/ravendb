using System.Net;
using System.Security.Cryptography;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
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
    }

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
