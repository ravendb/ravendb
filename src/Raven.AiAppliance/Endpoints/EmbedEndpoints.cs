using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.Agents;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;

namespace Raven.AiAppliance.Endpoints;

/// <summary>
/// Public iFrame channel (design §1.4 / §3.5): the embed page + its chat stream,
/// keyed by the public <c>widgetId</c> (no login). Map before the SPA fallback in
/// <c>Program.cs</c> or <c>{*path:nonfile}</c> swallows it.
/// </summary>
public static class EmbedEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/embed/{widgetId}", ServeEmbedPageAsync)
            .WithName("embed.page")
            .ExcludeFromDescription();

        app.MapPost("/embed/{widgetId}/chat", StreamEmbedChatAsync)
            .WithName("embed.chat")
            .Accepts<EmbedChatRequest>("application/json")
            .ExcludeFromDescription();
    }

    private static async Task ServeEmbedPageAsync(
        string widgetId,
        IDocumentStore store,
        HttpContext ctx)
    {
        var ct = ctx.RequestAborted;

        var resolved = await TryResolveEnabledChannelAsync(ctx, store, widgetId, ct);
        if (resolved is null)
            return;

        var (_, channel) = resolved.Value;

        // frame-ancestors from the configured origins; empty list = embeddable
        // anywhere (M1 contract). The chat POST also runs the Origin check.
        if (channel.AllowedOrigins.Length > 0)
            ctx.Response.Headers["Content-Security-Policy"] = $"frame-ancestors {string.Join(' ', channel.AllowedOrigins)}";

        ctx.Response.ContentType = "text/html; charset=utf-8";
        await ctx.Response.WriteAsync(BuildEmbedHtml(widgetId, channel.DisplayName), ct);
    }

    private static async Task StreamEmbedChatAsync(
        string widgetId,
        EmbedChatRequest body,
        IDocumentStore store,
        IAgentRouter router,
        ILogger<EmbedLogger> logger,
        HttpContext ctx)
    {
        var ct = ctx.RequestAborted;

        if (body is null || string.IsNullOrWhiteSpace(body.Prompt))
        {
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("prompt is required"), ct);
            return;
        }

        var resolved = await TryResolveEnabledChannelAsync(ctx, store, widgetId, ct);
        if (resolved is null)
            return;

        var (app, channel) = resolved.Value;

        // M1b Origin defense-in-depth (see IsOriginAllowed).
        if (IsOriginAllowed(ctx.Request, channel.AllowedOrigins) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("origin not allowed", Code: "origin_forbidden"), ct);
            return;
        }

        // Turn 1: mint a random chats/{guid} (not RavenDB's enumerable
        // sequential id — A2). Turn 2+: take the client's echoed id, pinned to
        // the chats/ prefix; a guessed id is random-space, so it just misses.
        string conversationId;
        if (string.IsNullOrWhiteSpace(body.ConversationId))
        {
            conversationId = "chats/" + Guid.NewGuid().ToString("N");
        }
        else if (AgentRouter.TryNormalizeConversationId(body.ConversationId, out conversationId, out var conversationError) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse(conversationError!), ct);
            return;
        }
        else if (conversationId.EndsWith('/') || conversationId.EndsWith('|'))
        {
            // A trailing '/' or '|' makes RavenDB auto-allocate a sequential/identity
            // id (enumerable — A2). A real continuation id never ends that way.
            ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("conversationId must be a complete id, not an allocation prefix"), ct);
            return;
        }

        // The channel's agent can be deleted out from under it; fail clean as
        // 404 before the stream opens (public surface collapses all misses to 404).
        var config = await AgentLookup.FindAsync(store, app.Database, channel.AgentId, ct);
        if (config is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        NdjsonStream.SetHeaders(ctx);
        try
        {
            var result = await router.RunAsync(
                new AgentRequest(app.Database, config.Identifier, conversationId, body.Prompt, Parameters: null),
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                ct);

            // Echo the (unguessable, random) id so the client can continue.
            await NdjsonStream.WriteLineAsync(ctx, new
            {
                type = "done",
                answer = result.Answer,
                conversationId = result.ConversationId,
            });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // End-user closed the widget mid-stream.
        }
        catch (Exception e)
        {
            logger.LogError(e, "embed chat failed for widgetId={WidgetId}", widgetId);
            try
            {
                await NdjsonStream.WriteLineAsync(ctx, new { type = "error", message = "Chat failed. See server logs for details." });
            }
            catch
            {
                // Response may already be partially flushed.
            }
        }
    }

    /// <summary>
    /// M1b: 403 a present-but-disallowed Origin. Browser-script defense only —
    /// non-browser callers omit Origin and pass (the random id is the real guard).
    /// Empty list skips (M1). The appliance's own origin is always allowed,
    /// case-insensitively. Known gap: breaks behind a TLS proxy (no UseForwardedHeaders).
    /// </summary>
    private static bool IsOriginAllowed(HttpRequest request, string[] allowedOrigins)
    {
        if (allowedOrigins.Length == 0)
            return true;

        var origin = request.Headers.Origin.ToString();
        if (string.IsNullOrEmpty(origin))
            return true;

        foreach (var allowed in allowedOrigins)
        {
            if (string.Equals(origin, allowed, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        var self = $"{request.Scheme}://{request.Host}";
        return string.Equals(origin, self, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Resolves the widget, writing 404 (unresolved) or 410 (disabled)
    /// and returning null in those cases; otherwise the live (App, Channel).</summary>
    private static async Task<(App app, Channel channel)?> TryResolveEnabledChannelAsync(
        HttpContext ctx, IDocumentStore store, string widgetId, CancellationToken ct)
    {
        var resolved = await ResolveAsync(store, widgetId, ct);
        if (resolved is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return null;
        }

        if (resolved.Value.channel.Enabled == false)
        {
            // 410 Gone: the widget existed but the operator paused it.
            ctx.Response.StatusCode = StatusCodes.Status410Gone;
            return null;
        }

        return resolved;
    }

    /// <summary>Resolves a widgetId by hopping config DB → app DB:
    /// <c>widget-index/{widgetId}</c> → <c>apps/{slug}</c> → <c>channels/{widgetId}</c>.
    /// Null on any miss (callers surface all misses as 404).</summary>
    private static async Task<(App app, Channel channel)?> ResolveAsync(
        IDocumentStore store, string widgetId, CancellationToken ct)
    {
        App? app;
        using (var cfg = store.OpenAsyncSession())
        {
            var index = await cfg.LoadAsync<WidgetIndex>($"widget-index/{widgetId}", ct);
            if (string.IsNullOrEmpty(index?.Slug))
                return null;

            // Depends on the widget-index result; reuses the config-DB session.
            app = await cfg.LoadAsync<App>($"apps/{index.Slug}", ct);
        }

        if (app is null)
            return null;

        Channel? channel;
        using (var session = store.OpenAsyncSession(app.Database))
            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + widgetId, ct);

        if (channel is null)
            return null;

        // iFrame-only surface — a non-IFrame channel sharing the prefix is a miss.
        if (channel.Type != ChannelType.IFrame)
            return null;

        return (app, channel);
    }

    private static string BuildEmbedHtml(string widgetId, string displayName)
    {
        var title = WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(displayName) ? "AI Assistant" : displayName);
        return EmbedHtmlTemplate
            .Replace("__WIDGET_ID__", widgetId)
            .Replace("__TITLE__", title);
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class EmbedLogger;

    // Self-contained vanilla page. Placeholders are string-replaced (title is
    // HTML-encoded; widgetId is base64url-safe) so JS/CSS braces need no escaping.
    private const string EmbedHtmlTemplate = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
  :root { --ai-bubble-bg: #f1f5f9; --ai-user-bg: #2563eb; --ai-user-fg: #fff; }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
  #ai-chat { display: flex; flex-direction: column; height: 100%; }
  #ai-chat-header { padding: 12px 16px; font-weight: 600; border-bottom: 1px solid #e2e8f0; }
  #ai-chat-feed { flex: 1; overflow-y: auto; padding: 12px 16px; }
  .row { margin: 6px 0; padding: 8px 12px; border-radius: 12px; max-width: 80%; white-space: pre-wrap; }
  .row.user { background: var(--ai-user-bg); color: var(--ai-user-fg); margin-left: auto; }
  .row.agent { background: var(--ai-bubble-bg); }
  #ai-chat-form { display: flex; gap: 8px; padding: 12px 16px; border-top: 1px solid #e2e8f0; }
  #ai-chat-input { flex: 1; padding: 10px 12px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; }
  #ai-chat-form button { padding: 10px 16px; border: 0; border-radius: 8px; background: var(--ai-user-bg); color: #fff; cursor: pointer; }
</style>
</head>
<body>
<div id="ai-chat" data-widget-id="__WIDGET_ID__">
  <div id="ai-chat-header">__TITLE__</div>
  <div id="ai-chat-feed"></div>
  <form id="ai-chat-form">
    <input id="ai-chat-input" autocomplete="off" placeholder="Ask a question..." aria-label="Ask a question">
    <button type="submit">Send</button>
  </form>
</div>
<script>
const widgetId = "__WIDGET_ID__";
const feed = document.getElementById("ai-chat-feed");
const form = document.getElementById("ai-chat-form");
const input = document.getElementById("ai-chat-input");
// Server-minted id from the previous done frame; in-memory only (reload = fresh chat).
let conversationId = null;

function addRow(cls, text) {
  const div = document.createElement("div");
  div.className = "row " + cls;
  div.textContent = text;
  feed.appendChild(div);
  feed.scrollTop = feed.scrollHeight;
  return div;
}

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const prompt = input.value.trim();
  if (!prompt) return;
  input.value = "";
  addRow("user", prompt);
  const agentRow = addRow("agent", "");
  try {
    const resp = await fetch(`/embed/${widgetId}/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt, conversationId })
    });
    if (!resp.ok || !resp.body) { agentRow.textContent = "[error] HTTP " + resp.status; return; }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let nl;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        const msg = JSON.parse(line);
        if (msg.type === "chunk") agentRow.textContent += msg.text;
        else if (msg.type === "done") {
          if (msg.conversationId) conversationId = msg.conversationId;
          // If nothing streamed incrementally, fall back to the final answer
          // so the reply is always shown (some models return it in one shot).
          if (!agentRow.textContent && msg.answer && msg.answer.reply) agentRow.textContent = msg.answer.reply;
        }
        else if (msg.type === "error") agentRow.textContent = "[error] " + msg.message;
      }
    }
  } catch (err) {
    agentRow.textContent = "[error] " + err;
  }
});
</script>
</body>
</html>
""";
}
