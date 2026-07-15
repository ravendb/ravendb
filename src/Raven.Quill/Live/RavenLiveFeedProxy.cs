using System.Net.WebSockets;
using Raven.Client.Documents;

namespace Raven.Quill.Live;

internal static class RavenLiveFeedProxy
{
    // 12h backstop so a forgotten dashboard tab can't hold a feed forever
    private static readonly TimeSpan MaxLifetime = TimeSpan.FromHours(12);

    public static async Task RelayAsync(
        WebSocket browser,
        IDocumentStore store,
        string database,
        string relativeLivePath,
        CancellationToken ct)
    {
        var upstreamUri = await BuildUpstreamUriAsync(store, database, relativeLivePath);

        // bridge is the mTLS proxy: the browser can't present a client cert
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
            return;
        }
        catch (WebSocketException)
        {
            if (browser.State == WebSocketState.Open)
            {
                try
                {
                    await browser.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, "upstream feed unavailable", ct);
                }
                catch (WebSocketException)
                {
                    /* browser already gone — best effort */
                }
                catch (OperationCanceledException)
                {
                    /* request aborted mid-close — best effort */
                }
            }

            return;
        }

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
        }
        catch (WebSocketException)
        {
        }

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
        string baseUrl;
        try
        {
            var (_, node) = await store.GetRequestExecutor(database).GetPreferredNode();
            baseUrl = node.Url.TrimEnd('/');
        }
        catch (Exception)
        {
            baseUrl = store.Urls[0].TrimEnd('/');
        }

        var wsBase = baseUrl.StartsWith("https", StringComparison.OrdinalIgnoreCase)
            ? string.Concat("wss", baseUrl.AsSpan("https".Length))
            : string.Concat("ws", baseUrl.AsSpan("http".Length));

        return new Uri($"{wsBase}/databases/{database}/{relativeLivePath.TrimStart('/')}");
    }
}
