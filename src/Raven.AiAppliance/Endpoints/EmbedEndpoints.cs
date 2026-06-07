using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.AiAppliance.Agents;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Endpoints.Helpers;
using Raven.AiAppliance.Schema;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;

namespace Raven.AiAppliance.Endpoints;

/// <summary>
/// Public customer-facing iFrame channel (design §1.4 / §3.5). Two routes —
/// the embed page and its chat stream — keyed by the public <c>widgetId</c>
/// (no login). Conversation continuation requires the opaque <c>cnv_</c>
/// token minted on turn 1 (RavenDB-26700 auth follow-up; closes ayende's A2 —
/// raw <c>chats/</c> ids are never accepted or exposed), plus the M1b Origin
/// defense-in-depth check. Turn 1 itself stays ungated (accepted demo
/// posture; rate limiting deferred). Deliberately mapped OUTSIDE the
/// <c>/api</c> group so the readiness gate and dashboard auth don't apply; it
/// MUST be registered before the SPA fallback in <c>Program.cs</c> or the
/// <c>{*path:nonfile}</c> fallback would swallow it.
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

        var resolved = await TryResolveEnabledChannelAsync(ctx, store, widgetId, bindingId: null, ct);
        if (resolved is null)
            return;

        var (_, channel, _) = resolved.Value;

        // Best-effort hardening: constrain who may frame this page to the
        // operator-configured origins; the host page's own CSP still governs
        // actual loading. Decided 2026-06-04: an EMPTY origins list
        // intentionally emits no frame-ancestors at all — the widget is
        // embeddable from anywhere (M1 documented contract). The
        // /embed/{widgetId}/chat POST additionally runs the M1b Origin
        // defense-in-depth check (see IsOriginAllowed) and requires the
        // conversation token for continuation.
        if (channel.AllowedOrigins.Length > 0)
            ctx.Response.Headers["Content-Security-Policy"] = $"frame-ancestors {string.Join(' ', channel.AllowedOrigins)}";

        ctx.Response.ContentType = "text/html; charset=utf-8";
        await ctx.Response.WriteAsync(BuildEmbedHtml(widgetId, channel.DisplayName), ct);
    }

    /// <summary>How long a minted conversation token can resume its
    /// conversation (read-validated on the binding doc). This is TOKEN
    /// VALIDITY — distinct from session retention (deferred follow-up).</summary>
    private static readonly TimeSpan ConversationTtl = TimeSpan.FromHours(24);

    private const string ConversationTokenPrefix = "cnv_";

    private static async Task StreamEmbedChatAsync(
        string widgetId,
        EmbedChatRequest body,
        IDocumentStore store,
        IAgentRouter router,
        IAgentSchemaRegistry schemas,
        ConversationBindings bindings,
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

        // Shape-check the token up front (pure string work; L2: junk and
        // id-trickery never reach doc-id space) so a well-formed token's
        // binding doc can ride the channel load's app-DB round trip — one
        // round trip per continuation turn instead of two (I1, impl review
        // 2026-06-07). The RESPONSE order contract is unchanged:
        // 404/410 resolve -> 403 origin -> 401 token.
        var token = body.ConversationToken?.Trim();
        var hasToken = string.IsNullOrEmpty(token) == false;
        var bindingId = hasToken && IsWellFormedToken(token!)
            ? ConversationBinding.MakeId(widgetId, token!)
            : null;

        var resolved = await TryResolveEnabledChannelAsync(ctx, store, widgetId, bindingId, ct);
        if (resolved is null)
            return;

        var (app, channel, binding) = resolved.Value;

        // M1b origin defense-in-depth — see IsOriginAllowed for the honest
        // accounting of what this does and does not protect against.
        if (IsOriginAllowed(ctx.Request, channel.AllowedOrigins) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("origin not allowed", Code: "origin_forbidden"), ct);
            return;
        }

        // Continuation: the opaque cnv_ token resolves to the hidden chats/ id
        // through its (pre-loaded) conversation-binding doc. The raw
        // conversation id is never accepted from the client (A2:
        // server-allocated chats/ ids are sequential/enumerable — a guessed id
        // could read another user's chat).
        string? conversationId = null;
        if (hasToken)
        {
            // Malformed shape never loaded a binding -> same 401 as a miss.
            var (resolvedId, errorCode) = bindingId is null
                ? (null, ConversationBindings.UnknownCode)
                : bindings.Validate(binding);
            if (resolvedId is null)
            {
                await WriteAuthErrorAsync(ctx, errorCode!, ct);
                return;
            }

            conversationId = resolvedId;
        }

        // The channel's stored AgentId can drift out of the in-process registry
        // (agent renamed/removed across versions). Check before the stream opens
        // so the failure is a clean status instead of 200 + an error frame — and
        // use 404, not 400: the agent id is server-side state, not client input,
        // and the public embed surface deliberately collapses all failure modes
        // into 404 (mirrors ResolveAsync). Runs BEFORE minting (M3) so a request
        // that is about to 404 never writes a binding + compare-exchange guard.
        if (schemas.TryGet(channel.AgentId, out var schema) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        // Turn 1: mint the conversation server-side — a random hidden chats/
        // id behind a random public token (both crypto-random, see RandomIds).
        // The binding is written before the run so the conversation survives a
        // failed first reply; an orphan from a crash is rejected by the
        // validity window and is otherwise inert.
        string? mintedToken = null;
        if (conversationId is null)
        {
            mintedToken = RandomIds.NewId(ConversationTokenPrefix);
            conversationId = await bindings.GetOrCreateAsync(
                app.Database,
                ConversationBinding.MakeId(widgetId, mintedToken),
                widgetId,
                static () => RandomIds.NewId("chats/"),
                ConversationTtl,
                ct);
        }

        NdjsonStream.SetHeaders(ctx);
        try
        {
            // The token rides its own leading frame (not the done frame): the
            // client must keep the conversation even when the reply errors out
            // mid-stream, and turn 2+ needs no re-send (the client already
            // holds it).
            if (mintedToken is not null)
                await NdjsonStream.WriteLineAsync(ctx, new { type = "conversation", conversationToken = mintedToken });

            var result = await router.RunAsync(
                new AgentRequest(app.Database, schema.Identifier, conversationId, body.Prompt, Parameters: null),
                async chunk => await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk }),
                ct);

            // A2: the done frame deliberately does NOT echo the conversation
            // id — the opaque token is the only continuation handle.
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
            // L1: widgetId is public; the conversation token must never be
            // logged (the binding id embeds it — exception messages from the
            // binding path use a redacted prefix form).
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

    /// <summary>Writes the embed-auth 401: same status for unknown / expired /
    /// malformed tokens (the widget reacts identically — clear the token and
    /// let the next submit start fresh); the <see cref="ApiErrorResponse.Code"/>
    /// distinguishes them for tests and diagnostics. Only the token holder can
    /// probe the difference, so the code is not an oracle.</summary>
    private static async Task WriteAuthErrorAsync(HttpContext ctx, string code, CancellationToken ct)
    {
        ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
        await ctx.Response.WriteAsJsonAsync(
            new ApiErrorResponse("unknown, expired or malformed conversation token", Code: code), ct);
    }

    /// <summary>
    /// L2: accepts exactly the minted token shape — <c>cnv_</c> +
    /// <see cref="RandomIds.IsValidSuffix"/> (the generator's own validate
    /// twin, so gate and emit can't drift apart). Shape only, not auth: the
    /// binding-doc load downstream is the authority (and doc-id lookups are
    /// case-insensitive — L3 — so a case-mangled valid token still resolves;
    /// that costs nothing).
    /// </summary>
    private static bool IsWellFormedToken(string token)
    {
        return token.StartsWith(ConversationTokenPrefix, StringComparison.Ordinal)
            && RandomIds.IsValidSuffix(token.AsSpan(ConversationTokenPrefix.Length));
    }

    /// <summary>
    /// M1b origin defense-in-depth on the chat POST. Honest accounting (M1,
    /// security review 2026-06-04): this constrains BROWSER-SCRIPT abuse only —
    /// non-browser bots omit <c>Origin</c> and pass (the token is the real
    /// control), and cross-origin browser <c>fetch</c> is already dead without
    /// it (no CORS middleware → the preflight fails). Empty
    /// <c>AllowedOrigins</c> = the documented open-embed contract (M1): skip.
    /// The embed page itself POSTs with the appliance's own origin, so that is
    /// always allowed — compared case-insensitively (M2: <c>Request.Host</c>
    /// casing is not guaranteed across clients). M2 known limitation: behind a
    /// TLS-terminating proxy <c>Request.Scheme</c> stays <c>http</c> (Kestrel
    /// listens plain HTTP; no <c>UseForwardedHeaders</c>), so the self-origin
    /// compare would 403 the appliance's own widget — the demo posture is
    /// direct-port access; <c>UseForwardedHeaders</c> is the future fix.
    /// </summary>
    private static bool IsOriginAllowed(HttpRequest request, string[] allowedOrigins)
    {
        if (allowedOrigins.Length == 0)
            return true;

        var origin = request.Headers.Origin.ToString();
        if (string.IsNullOrEmpty(origin))
            return true;

        // Case-insensitive on purpose (C1, Copilot review PR #12): scheme and
        // host are case-insensitive per RFC 3986, so case can never
        // distinguish two origins — IgnoreCase removes false-denies for
        // unusually-cased clients and cannot false-allow. (Stored origins are
        // already lowercased by Uri at provision; the INCOMING header's casing
        // is only guaranteed for conformant browsers.) Same rationale as the
        // self-origin compare below (M2).
        foreach (var allowed in allowedOrigins)
        {
            if (string.Equals(origin, allowed, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        var self = $"{request.Scheme}://{request.Host}";
        return string.Equals(origin, self, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Resolves the widget and enforces the public-route status contract shared
    /// by both embed handlers: writes <c>404</c> when the widget can't be
    /// resolved, <c>410 Gone</c> when it's disabled, and returns the
    /// <c>(App, Channel, ConversationBinding?)</c> only when the channel is
    /// live. Returns null (with the status already written) otherwise. The
    /// optional <paramref name="bindingId"/> lets the chat path batch the
    /// conversation-binding load into the channel's app-DB round trip (I1);
    /// the page path passes null.
    /// </summary>
    private static async Task<(App app, Channel channel, ConversationBinding? binding)?> TryResolveEnabledChannelAsync(
        HttpContext ctx, IDocumentStore store, string widgetId, string? bindingId, CancellationToken ct)
    {
        var resolved = await ResolveAsync(store, widgetId, bindingId, ct);
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

    /// <summary>
    /// Resolves a public widgetId to its per-app DB + channel doc. The embed
    /// routes have only the widgetId, and the bridge's default store targets
    /// the config DB, so we hop: <c>widget-index/{widgetId}</c> (config) → slug →
    /// <c>apps/{slug}</c> (config) → <c>channels/{widgetId}</c> (app DB).
    /// Returns null on any miss — the public page must not distinguish the
    /// failure modes (all surface as 404).
    /// </summary>
    private static async Task<(App app, Channel channel, ConversationBinding? binding)?> ResolveAsync(
        IDocumentStore store, string widgetId, string? bindingId, CancellationToken ct)
    {
        App? app;
        using (var cfg = store.OpenAsyncSession())
        {
            var index = await cfg.LoadAsync<WidgetIndex>($"widget-index/{widgetId}", ct);
            if (string.IsNullOrEmpty(index?.Slug))
                return null;

            // The apps/{slug} load depends on the widget-index result, so it
            // can't be batched — but it reuses the same config-DB session.
            app = await cfg.LoadAsync<App>($"apps/{index.Slug}", ct);
        }

        if (app is null)
            return null;

        Channel? channel;
        ConversationBinding? binding = null;
        using (var session = store.OpenAsyncSession(app.Database))
        {
            if (bindingId is null)
            {
                channel = await session.LoadAsync<Channel>(Channel.IdPrefix + widgetId, ct);
            }
            else
            {
                // I1 (impl review 2026-06-07): a continuation turn needs the
                // channel AND the binding from the same app DB — batch both
                // into ONE server round trip via lazy loads. Loading the
                // binding speculatively (before the 410/origin gates) is
                // harmless: same round trip, and the gates still answer in
                // contract order.
                var lazyChannel = session.Advanced.Lazily.LoadAsync<Channel>(Channel.IdPrefix + widgetId);
                var lazyBinding = session.Advanced.Lazily.LoadAsync<ConversationBinding>(bindingId);
                await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync(ct);
                channel = await lazyChannel.Value;
                binding = await lazyBinding.Value;
            }
        }

        if (channel is null)
            return null;

        // /embed/{widgetId} is the iFrame public surface — never serve a
        // non-IFrame channel doc (e.g. a future Telegram/WhatsApp channel that
        // shares the channels/ prefix). Treat it as a miss -> 404.
        if (channel.Type != ChannelType.IFrame)
            return null;

        return (app, channel, binding);
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

    // Self-contained vanilla page (no framework, no external assets). widgetId
    // is base64url-safe so it's substituted directly; the title is HTML-encoded
    // before substitution. Placeholders are replaced (not C# interpolation) so
    // the JS/CSS braces need no escaping.
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
// The opaque continuation token from the first turn's "conversation" frame.
// Lives in this variable ONLY — never localStorage/sessionStorage: persisting
// it would outlive the server-side validity window and widen XSS exposure for
// no gain (a page reload simply starts a fresh conversation).
let conversationToken = null;

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
      body: JSON.stringify({ prompt, conversationToken })
    });
    if (resp.status === 401) {
      // Token expired or invalidated. Inform only — NO auto-resubmit (a retry
      // loop against a persistent 401 would hammer the server); the user's
      // next submit starts a fresh conversation.
      conversationToken = null;
      agentRow.textContent = "Session expired — starting a new chat. Please send your message again.";
      return;
    }
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
        else if (msg.type === "conversation") conversationToken = msg.conversationToken;
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
