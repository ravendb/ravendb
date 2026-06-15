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
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;

namespace Raven.AiAppliance.Endpoints;

/// <summary>
/// Public iFrame channel (design §1.4 / §3.5), reworked for RavenDB-26775: the
/// URL carries an API-minted <c>token</c> (an <see cref="EmbedLink"/>), not a
/// static public widgetId. The token binds the agent parameters server-side,
/// carries a TTL + an N-invocation cap, and owns its conversation — so a visitor
/// can neither impersonate another customer (no <c>?customerId=</c>) nor spam
/// turns past the cap. Minting + revocation live in <see cref="EmbedLinksEndpoints"/>.
/// Map before the SPA fallback in <c>Program.cs</c> or <c>{*path:nonfile}</c>
/// swallows it.
/// </summary>
public static class EmbedEndpoints
{
    /// <summary>Name of the per-IP rate-limit policy applied to the public chat
    /// route (configured in <c>Program.cs</c>). RavenDB-26775 backstop.</summary>
    public const string ChatRateLimitPolicy = "embed-chat";

    public static void Map(WebApplication app)
    {
        app.MapGet("/embed/{token}", ServeEmbedPageAsync)
            .WithName("embed.page")
            .ExcludeFromDescription();

        app.MapPost("/embed/{token}/chat", StreamEmbedChatAsync)
            .WithName("embed.chat")
            .Accepts<EmbedChatRequest>("application/json")
            .RequireRateLimiting(ChatRateLimitPolicy)
            .ExcludeFromDescription();
    }

    private static async Task ServeEmbedPageAsync(
        string token,
        IDocumentStore store,
        HttpContext ctx)
    {
        var ct = ctx.RequestAborted;

        var resolved = await TryResolveLiveLinkAsync(ctx, store, token, ct);
        if (resolved is null)
            return;

        var (_, _, channel) = resolved.Value;

        // frame-ancestors from the configured origins; empty list = embeddable
        // anywhere (M1 contract). 'self' is always included so the appliance's own
        // UI can preview the widget.
        if (channel.AllowedOrigins.Length > 0)
            ctx.Response.Headers["Content-Security-Policy"] = $"frame-ancestors 'self' {string.Join(' ', channel.AllowedOrigins)}";

        // Keep the bearer token out of cross-origin referer logs.
        ctx.Response.Headers["Referrer-Policy"] = "no-referrer";

        ctx.Response.ContentType = "text/html; charset=utf-8";
        await ctx.Response.WriteAsync(BuildEmbedHtml(token, channel.DisplayName), ct);
    }

    private static async Task StreamEmbedChatAsync(
        string token,
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

        var resolved = await TryResolveLiveLinkAsync(ctx, store, token, ct);
        if (resolved is null)
            return;

        var (app, link, channel) = resolved.Value;

        // M1b Origin defense-in-depth (see IsOriginAllowed).
        if (IsOriginAllowed(ctx.Request, channel.AllowedOrigins) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("origin not allowed", Code: "origin_forbidden"), ct);
            return;
        }

        // Atomically reserve one invocation and mint/pin the link's conversation.
        // The cap is the link's structural rate limit; the conversation is
        // server-owned (no client-supplied conversation id on this surface).
        var gate = await ConsumeInvocationAsync(store, app.Database, token, ct);
        switch (gate.Status)
        {
            case GateStatus.Gone:
                ctx.Response.StatusCode = StatusCodes.Status410Gone;
                return;
            case GateStatus.Exhausted:
                ctx.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                await ctx.Response.WriteAsJsonAsync(
                    new ApiErrorResponse("this link has reached its usage limit", Code: "invocation_limit"), ct);
                return;
        }

        // The channel's agent can be deleted out from under it; fail clean as 404
        // before the stream opens (public surface collapses all misses to 404).
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
                // Parameters come from the link (bound at mint), never the request body.
                new AgentRequest(app.Database, config.Identifier, gate.ConversationId, body.Prompt, link.Parameters),
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                ct,
                resolved: config);

            await NdjsonStream.WriteLineAsync(ctx, new
            {
                type = "done",
                answer = result.Answer,
            });
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // End-user closed the widget mid-stream.
        }
        catch (Exception e)
        {
            logger.LogError(e, "embed chat failed for token={Token}", token);
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

    private enum GateStatus { Ok, Exhausted, Gone }

    private readonly record struct InvocationGate(GateStatus Status, string ConversationId)
    {
        public static InvocationGate Ok(string conversationId) => new(GateStatus.Ok, conversationId);
        public static readonly InvocationGate Exhausted = new(GateStatus.Exhausted, "");
        public static readonly InvocationGate Gone = new(GateStatus.Gone, "");
    }

    /// <summary>
    /// Atomically consumes one invocation against the link: re-checks the live
    /// link (revoked / expired / over-cap), mints the conversation id on the first
    /// turn, increments the count, and saves under optimistic concurrency so
    /// concurrent turns can't exceed <see cref="EmbedLink.MaxInvocations"/>. A
    /// concurrency clash retries against the fresh count.
    /// </summary>
    private static async Task<InvocationGate> ConsumeInvocationAsync(
        IDocumentStore store, string database, string token, CancellationToken ct)
    {
        const int maxAttempts = 8;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            using var session = store.OpenAsyncSession(database);
            session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;

            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token, ct);
            if (link is null || link.Revoked || link.ExpiresAt <= DateTime.UtcNow)
                return InvocationGate.Gone;

            if (link.InvocationCount >= link.MaxInvocations)
                return InvocationGate.Exhausted;

            if (string.IsNullOrEmpty(link.ConversationId))
                link.ConversationId = "chats/" + Guid.NewGuid().ToString("N");

            link.InvocationCount++;

            try
            {
                await session.SaveChangesAsync(ct);
                return InvocationGate.Ok(link.ConversationId);
            }
            catch (ConcurrencyException)
            {
                // Another concurrent turn won; reload and re-evaluate the cap.
            }
        }

        // Sustained contention on a single link is itself abuse-shaped — shed it.
        return InvocationGate.Exhausted;
    }

    /// <summary>
    /// M1b: 403 a present-but-disallowed Origin. Browser-script defense only —
    /// non-browser callers omit Origin and pass (the token is the real guard).
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

    /// <summary>Resolves the token and applies the liveness gates, writing 404
    /// (unresolved / malformed) or 410 (disabled channel / revoked / expired) and
    /// returning null in those cases; otherwise the live (App, EmbedLink, Channel).
    /// The invocation cap is enforced separately at chat time (429).</summary>
    private static async Task<(App app, EmbedLink link, Channel channel)?> TryResolveLiveLinkAsync(
        HttpContext ctx, IDocumentStore store, string token, CancellationToken ct)
    {
        if (EmbedLink.IsWellFormedToken(token) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return null;
        }

        var resolved = await ResolveAsync(store, token, ct);
        if (resolved is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return null;
        }

        var (_, link, channel) = resolved.Value;

        // 410 Gone: link existed but is no longer usable (channel paused, link
        // revoked, or TTL elapsed). All collapse to the same "this link is dead".
        if (channel.Enabled == false || link.Revoked || link.ExpiresAt <= DateTime.UtcNow)
        {
            ctx.Response.StatusCode = StatusCodes.Status410Gone;
            return null;
        }

        return resolved;
    }

    /// <summary>Resolves a token by hopping config DB → app DB:
    /// <c>link-index/{token}</c> → <c>apps/{slug}</c> → (<c>embed-links/{token}</c>,
    /// <c>channels/{widgetId}</c>). Null on any miss (callers surface as 404).</summary>
    private static async Task<(App app, EmbedLink link, Channel channel)?> ResolveAsync(
        IDocumentStore store, string token, CancellationToken ct)
    {
        App? app;
        using (var cfg = store.OpenAsyncSession())
        {
            var index = await cfg.LoadAsync<LinkIndex>(LinkIndex.IdPrefix + token, ct);
            if (string.IsNullOrEmpty(index?.Slug))
                return null;

            app = await cfg.LoadAsync<App>($"apps/{index.Slug}", ct);
        }

        if (app is null)
            return null;

        EmbedLink? link;
        Channel? channel;
        using (var session = store.OpenAsyncSession(app.Database))
        {
            link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token, ct);
            if (link is null)
                return null;

            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + link.WidgetId, ct);
        }

        // iFrame-only surface — a non-IFrame channel sharing the prefix is a miss.
        if (channel is null || channel.Type != ChannelType.IFrame)
            return null;

        return (app, link, channel);
    }

    private static string BuildEmbedHtml(string token, string displayName)
    {
        var title = WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(displayName) ? "AI Assistant" : displayName);
        return EmbedHtmlTemplate
            .Replace("__TOKEN__", token)
            .Replace("__TITLE__", title);
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class EmbedLogger;

    // Self-contained vanilla page. Placeholders are string-replaced (title is
    // HTML-encoded; token is hex-only) so JS/CSS braces need no escaping.
    private const string EmbedHtmlTemplate = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
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
<div id="ai-chat">
  <div id="ai-chat-header">__TITLE__</div>
  <div id="ai-chat-feed"></div>
  <form id="ai-chat-form">
    <input id="ai-chat-input" autocomplete="off" placeholder="Ask a question..." aria-label="Ask a question">
    <button type="submit">Send</button>
  </form>
</div>
<script>
// The token is the bearer credential and owns the conversation + the bound agent
// parameters server-side. This page sends only the prompt; the customer's
// minted link decided who the user is and how long it lives.
const token = "__TOKEN__";
const feed = document.getElementById("ai-chat-feed");
const form = document.getElementById("ai-chat-form");
const input = document.getElementById("ai-chat-input");

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
    const resp = await fetch(`/embed/${token}/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prompt })
    });
    if (resp.status === 410) { agentRow.textContent = "[this link is no longer active]"; return; }
    if (resp.status === 429) { agentRow.textContent = "[this link has reached its usage limit]"; return; }
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
