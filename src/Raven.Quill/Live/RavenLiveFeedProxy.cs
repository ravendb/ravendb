using System.Net.WebSockets;
using Raven.Client.Documents;

namespace Raven.Quill.Live;

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
        var upstreamUri = await BuildUpstreamUriAsync(store, database, relativeLivePath);

        using var upstream = new ClientWebSocket();
        if (store.Certificate is not null)
            upstream.Options.ClientCertificates.Add(store.Certificate);

        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(ct);
        lifetime.CancelAfter(MaxLifetime);
        var token = lifetime.Token;

        try
        {
            await upstream.ConnectAsync(upstreamUri, token);
        }
        catch (OperationCanceledException)
        {
            // Browser disconnected (or the 12h cap fired) before the upstream
            // feed opened — nothing to relay, nothing to close gracefully.
            return;
        }
        catch (WebSocketException)
        {
            // Upstream handshake failed (RavenDB unavailable, bad database/path).
            // The browser upgrade is already accepted, so close it cleanly with
            // an error status instead of letting the socket abort without a
            // close frame.
            if (browser.State == WebSocketState.Open)
            {
                try
                {
                    await browser.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, "upstream feed unavailable", ct);
                }
                catch (WebSocketException) { /* browser already gone — best effort */ }
                catch (OperationCanceledException) { /* request aborted mid-close — best effort */ }
            }

            return;
        }

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

        // Complete the close handshake on whichever sockets are still open. A
        // browser-initiated close otherwise ends in disposal aborts: no close
        // ack to the browser, and the upstream RavenDB socket logs an abort
        // instead of a graceful close. Best-effort, on a fresh short token —
        // the linked one is already cancelled by now.
        using var closeCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await CloseQuietlyAsync(browser, closeCts.Token);
        await CloseQuietlyAsync(upstream, closeCts.Token);
    }

    private static async Task CloseQuietlyAsync(WebSocket socket, CancellationToken token)
    {
        if (socket.State != WebSocketState.Open && socket.State != WebSocketState.CloseReceived)
            return;

        try
        {
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "closing", token);
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
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

    private static async Task<Uri> BuildUpstreamUriAsync(IDocumentStore store, string database, string relativeLivePath)
    {
        // Follow the client's topology selection instead of pinning Urls[0]: in
        // a cluster the request executor knows which node is healthy/preferred,
        // so the proxy dials the same node regular client requests use. (The
        // appliance runs single-node today, where this equals Urls[0].)
        string baseUrl;
        try
        {
            var (_, node) = await store.GetRequestExecutor(database).GetPreferredNode();
            baseUrl = node.Url.TrimEnd('/');
        }
        catch (Exception)
        {
            // Topology not initialized / executor unavailable — fall back to the
            // configured URL rather than failing the relay outright.
            baseUrl = store.Urls[0].TrimEnd('/');
        }

        var wsBase = baseUrl.StartsWith("https", StringComparison.OrdinalIgnoreCase)
            ? string.Concat("wss", baseUrl.AsSpan("https".Length))
            : string.Concat("ws", baseUrl.AsSpan("http".Length));

        return new Uri($"{wsBase}/databases/{database}/{relativeLivePath.TrimStart('/')}");
    }
}
