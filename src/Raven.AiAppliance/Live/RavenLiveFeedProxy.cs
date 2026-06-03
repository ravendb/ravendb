using System.Net.WebSockets;
using Raven.Client.Documents;

namespace Raven.AiAppliance.Live;

/// <summary>
/// Relays one of RavenDB's native live-telemetry WebSocket feeds (e.g.
/// <c>cdc-sink/performance/live</c>) from the per-app database out to a browser
/// WebSocket, verbatim. The browser can't present a client certificate, so the
/// bridge is the authenticated proxy: it dials RavenDB with the admin cert
/// (mTLS in secured prod; plain <c>ws://</c> in unsecured tests) and forwards
/// frames. Generic on the feed path so future dashboard metrics reuse it —
/// matching the Studio live-stats model rather than re-implementing it.
/// </summary>
internal static class RavenLiveFeedProxy
{
    // Backstop so a forgotten-open dashboard tab can't hold a feed forever; the
    // primary stop is the browser disconnecting (RequestAborted) or closing.
    private static readonly TimeSpan MaxLifetime = TimeSpan.FromHours(12);

    public static async Task RelayAsync(
        WebSocket browser,
        IDocumentStore store,
        string database,
        string relativeLivePath,
        CancellationToken ct)
    {
        var upstreamUri = BuildUpstreamUri(store, database, relativeLivePath);

        using var upstream = new ClientWebSocket();
        if (store.Certificate is not null)
            upstream.Options.ClientCertificates.Add(store.Certificate);

        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        lifetime.CancelAfter(MaxLifetime);
        var token = lifetime.Token;

        await upstream.ConnectAsync(upstreamUri, token);

        // One task pumps upstream -> browser; the other drains the browser so a
        // client-side close is noticed promptly. Whichever finishes first tears
        // the other down via the linked CTS.
        var pump = PumpAsync(upstream, browser, token);
        var drain = DrainAsync(browser, token);

        await Task.WhenAny(pump, drain);
        lifetime.Cancel();

        try
        {
            await Task.WhenAll(pump, drain);
        }
        catch (OperationCanceledException)
        {
            // Expected when the linked CTS cancels the loser.
        }
        catch (WebSocketException)
        {
            // Either side dropped — nothing actionable.
        }
    }

    private static async Task PumpAsync(WebSocket upstream, WebSocket browser, CancellationToken token)
    {
        var buffer = new byte[16 * 1024];
        try
        {
            while (upstream.State == WebSocketState.Open && browser.State == WebSocketState.Open)
            {
                var result = await upstream.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await browser.CloseAsync(WebSocketCloseStatus.NormalClosure, "upstream closed", token);
                    return;
                }

                await browser.SendAsync(
                    new ArraySegment<byte>(buffer, 0, result.Count),
                    result.MessageType,
                    result.EndOfMessage,
                    token);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    private static async Task DrainAsync(WebSocket browser, CancellationToken token)
    {
        var buffer = new byte[1024];
        try
        {
            while (browser.State == WebSocketState.Open)
            {
                var result = await browser.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                if (result.MessageType == WebSocketMessageType.Close)
                    return;
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
    }

    private static Uri BuildUpstreamUri(IDocumentStore store, string database, string relativeLivePath)
    {
        var baseUrl = store.Urls[0].TrimEnd('/');
        var wsBase = baseUrl.StartsWith("https", StringComparison.OrdinalIgnoreCase)
            ? string.Concat("wss", baseUrl.AsSpan("https".Length))
            : string.Concat("ws", baseUrl.AsSpan("http".Length));

        return new Uri($"{wsBase}/databases/{database}/{relativeLivePath.TrimStart('/')}");
    }
}
