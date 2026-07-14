using System.Linq;
using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Metrics;
using Raven.Quill.Wizard;

namespace Raven.Quill.Endpoints;

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

    // Resource CSP for the embed page: default-deny with only what the self-contained widget needs —
    // inline <style>/<script>, same-origin (+ data:) images/fonts, and same-origin fetch for the chat
    // stream. This contains operator-authored CSS so it can't @import or url(...) to a foreign origin
    // (beaconing/exfiltration). frame-ancestors is appended per-request from the channel's allowed
    // origins (see ServeEmbedPageAsync); IFrameCss.Sanitize handles the </style> HTML-breakout case.
    internal const string BaseCsp =
        "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; " +
        "img-src 'self' data:; font-src 'self' data:; connect-src 'self'; base-uri 'none'";

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

        var resolved = await TryResolveLiveLinkAsync(ctx, store, token, resolveStyle: true, ct);
        if (resolved is null)
            return;

        var (app, link, channel, style) = resolved.Value;

        // Contain the page — and, above all, operator-authored __CUSTOM_CSS__ — with a default-deny
        // resource policy, then append frame-ancestors from the configured origins (empty list =
        // embeddable anywhere, the M1 contract). 'self' (the public.* embed host) plus the operator
        // dashboard origin are always included so the appliance's own UI can preview the widget
        // cross-origin (the dashboard frames the public.* page).
        var csp = BaseCsp;
        if (channel.AllowedOrigins.Length > 0)
        {
            // 'self' (the public.* embed host) + the operator dashboard origin so the in-appliance
            // preview can frame the widget cross-origin, then the channel's configured origins. Skip the
            // dashboard origin when a configured origin already lists it (single-host dev collapses them).
            var dashboardOrigin = $"{ctx.Request.Scheme}://{ApplianceHost.WithSubdomain(ctx.Request.Host, "dashboard").ToUriComponent()}";
            var head = Array.IndexOf(channel.AllowedOrigins, dashboardOrigin) >= 0 ? "'self'" : $"'self' {dashboardOrigin}";
            csp += $"; frame-ancestors {head} {string.Join(' ', channel.AllowedOrigins)}";
        }

        ctx.Response.Headers["Content-Security-Policy"] = csp;

        // Keep the bearer token out of cross-origin referer logs.
        ctx.Response.Headers["Referrer-Policy"] = "no-referrer";

        // Prior turns for this link's conversation, rendered into the page so a returning
        // visitor sees their history (mirrors the operator Conversations view). Best-effort:
        // a fresh link (no conversation yet) or a read failure yields an empty feed.
        var historyJson = await BuildHistoryJsonAsync(store, app.Database, link.ConversationId, ct);

        ctx.Response.ContentType = "text/html; charset=utf-8";
        await ctx.Response.WriteAsync(BuildEmbedHtml(token, channel.DisplayName, style, historyJson), ct);
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

        var resolved = await TryResolveLiveLinkAsync(ctx, store, token, resolveStyle: false, ct);
        if (resolved is null)
            return;

        var (app, link, channel, _) = resolved.Value;

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
            // The gate already reserved an invocation, but the turn never runs —
            // refund it, same as a pre-stream failure (don't burn the grant on a
            // server-side agent-deletion the caller can't see or control).
            await RefundInvocationAsync(store, app.Database, token, logger);
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        NdjsonStream.SetHeaders(ctx);
        var streamedAny = false;
        try
        {
            var result = await router.RunAsync(
                // Parameters come from the link (bound at mint), never the request body.
                new AgentRequest(app.Database, config.Identifier, gate.ConversationId, body.Prompt, link.Parameters),
                async chunk =>
                {
                    streamedAny = true;
                    await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk });
                },
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
            // End-user closed the widget mid-stream — the invocation stays consumed
            // (a client-driven abort must not become a free retry / cost-without-count).
        }
        catch (Exception e)
        {
            logger.LogError(e, "embed chat failed for tokenPrefix={TokenPrefix}", EmbedLink.RedactToken(token));

            // Pre-stream failure (LLM 401/5xx, timeout, agent error): nothing was
            // streamed, so the conversation never advanced — refund the reserved
            // invocation so a transient upstream failure doesn't permanently burn the
            // grant. A mid-stream failure (streamedAny) stays consumed.
            if (streamedAny == false)
                await RefundInvocationAsync(store, app.Database, token, logger);

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
    /// Best-effort refund of one reserved invocation after a pre-stream failure (the
    /// turn never advanced the conversation). Decrements under optimistic concurrency
    /// and retries on a clash; any error is swallowed (over-counting on a rare race is
    /// fine, and a refund failure must never escape into the chat error path). Uses
    /// <see cref="CancellationToken.None"/> so the compensating write still lands even
    /// if the request was aborted.
    /// </summary>
    private static async Task RefundInvocationAsync(
        IDocumentStore store, string database, string token, ILogger<EmbedLogger> logger)
    {
        const int maxAttempts = 4;
        try
        {
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                using var session = store.OpenAsyncSession(database);
                session.Advanced.OptimisticConcurrencyMode = OptimisticConcurrencyMode.Writes;

                var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token, CancellationToken.None);
                if (link is null || link.InvocationCount <= 0)
                    return;

                link.InvocationCount--;

                try
                {
                    await session.SaveChangesAsync(CancellationToken.None);
                    return;
                }
                catch (ConcurrencyException)
                {
                    // A concurrent turn moved the count; reload and retry.
                }
            }
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "failed to refund invocation for tokenPrefix={TokenPrefix}", EmbedLink.RedactToken(token));
        }
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
    private static async Task<(App app, EmbedLink link, Channel channel, ResolvedIFrameStyle style)?> TryResolveLiveLinkAsync(
        HttpContext ctx, IDocumentStore store, string token, bool resolveStyle, CancellationToken ct)
    {
        if (EmbedLink.IsWellFormedToken(token) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return null;
        }

        var resolved = await ResolveAsync(store, token, resolveStyle, ct);
        if (resolved is null)
        {
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return null;
        }

        var (_, link, channel, _) = resolved.Value;

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
    /// <c>channels/{widgetId}</c>). Null on any miss (callers surface as 404). When
    /// <paramref name="resolveStyle"/> is set (the page render path) the effective embed style is
    /// resolved on the same app-DB session — the channel's own <see cref="Channel.Style"/> if
    /// chosen, else the app-level <see cref="IFrameStyleDefaults"/> — so rendering needs no second
    /// session. The chat path passes <c>false</c> and skips that load.</summary>
    private static async Task<(App app, EmbedLink link, Channel channel, ResolvedIFrameStyle style)?> ResolveAsync(
        IDocumentStore store, string token, bool resolveStyle, CancellationToken ct)
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
        ResolvedIFrameStyle style = default;
        using (var session = store.OpenAsyncSession(app.Database))
        {
            link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token, ct);
            if (link is null)
                return null;

            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + link.WidgetId, ct);

            // Resolve the embed style while the app-DB session is open (render path only): the channel's
            // own choice wins, otherwise fall back to the app-level default, loaded only when needed. This
            // keeps the page render to a single app-DB session instead of opening a second one.
            if (resolveStyle && channel is { Type: ChannelType.IFrame })
            {
                var defaults = IFrameStyleResolution.OwnStyle(channel) is null
                    ? await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct)
                    : null;
                style = IFrameStyleResolution.ForChannel(channel, defaults);
            }
        }

        // iFrame-only surface — a non-IFrame channel sharing the prefix is a miss.
        if (channel is null || channel.Type != ChannelType.IFrame)
            return null;

        return (app, link, channel, style);
    }

    private static string BuildEmbedHtml(string token, string displayName, ResolvedIFrameStyle style, string historyJson)
    {
        var title = WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(displayName) ? "AI Assistant" : displayName);
        // Substitute the trusted placeholders (token, base styles) first, then the operator/visitor-
        // controlled ones (custom CSS, title, then the conversation history) last — so nothing an
        // operator or visitor can set can reintroduce __TOKEN__ or another placeholder for a later
        // Replace to expand. In particular the bearer token can never leak into the title or history.
        return EmbedHtmlTemplate
            .Replace("__TOKEN__", token)
            .Replace("__BASE_CSS__", BuildWidgetBaseCss(style.Style))
            .Replace("__CUSTOM_CSS__", IFrameCss.Sanitize(style.CustomCss))
            .Replace("__TITLE__", title)
            .Replace("__HISTORY__", historyJson);
    }

    /// <summary>Serializes the conversation's prior turns as a script-safe JSON array
    /// (<c>[{role,text}]</c>) for the embed page. Best-effort: a fresh link (no conversation yet)
    /// or a read failure yields <c>[]</c> so the widget still renders. The default System.Text.Json
    /// encoder escapes &lt;, &gt; and &amp;, so the array is safe to inline inside &lt;script&gt;.</summary>
    private static async Task<string> BuildHistoryJsonAsync(
        IDocumentStore store, string database, string? conversationId, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(conversationId))
            return "[]";
        try
        {
            var result = await store.AI.ForDatabase(database).GetConversationMessagesAsync(conversationId, ct);
            if (result is null)
                return "[]";
            var turns = MetricsReadService.MapTranscript(result.Messages)
                .Select(t => new { role = t.Role, text = t.Text });
            return JsonSerializer.Serialize(turns);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return "[]";
        }
    }

    /// <summary>Builds the dashboard's customization preview: the same base styles
    /// and markup as the live embed, with sample chat bubbles and an empty
    /// <c>&lt;style id="raven-custom"&gt;</c> slot the dashboard fills client-side,
    /// but no live chat script. Sharing <see cref="WidgetBaseCss"/> keeps the
    /// preview faithful to the real page.</summary>
    internal static string BuildPreviewHtml(string? displayName)
    {
        var title = WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(displayName) ? "AI Assistant" : displayName);
        return PreviewHtmlTemplate
            .Replace("__TITLE__", title)
            .Replace("__BASE_CSS__", WidgetBaseCss);
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class EmbedLogger;

    // Base widget styles, shared verbatim by the live embed page, the dashboard preview
    // skeleton, and the styling editor's starter template, so none of the three can drift.
    // Injected into a <style> element via __BASE_CSS__ (kept out of the raw-string templates
    // so the CSS braces need no escaping). Operator CSS follows in a second <style> block so
    // it overrides these. The :root block is generated from IFrameStyleVariables per built-in
    // preset (Custom layers over the Light block), the single source for the shipped values.
    internal static string BuildWidgetBaseCss(IFrameStyle style) =>
        IFrameStyleVariables.BuildRootBlock(style) + "\n" + WidgetBaseCssRules;

    // The Light-preset base stylesheet: what the dashboard preview skeleton ships and the
    // custom-CSS editor pre-fills as its starter template.
    internal static readonly string WidgetBaseCss = BuildWidgetBaseCss(IFrameStyle.Light);

    private const string WidgetBaseCssRules = """
                                                * { box-sizing: border-box; }
                                                html, body { height: 100%; margin: 0; background: var(--ai-bg); color: var(--ai-fg); font-family: var(--ai-font-family); }
                                                #ai-chat { display: flex; flex-direction: column; height: 100%; }
                                                #ai-chat-header { padding: 12px 16px; font-weight: 600; border-bottom: 1px solid var(--ai-border-color); }
                                                #ai-chat-feed { flex: 1; overflow-y: auto; padding: 12px 16px; }
                                                .row { margin: 6px 0; padding: 8px 12px; border-radius: var(--ai-radius-bubble); max-width: 80%; white-space: pre-wrap; }
                                                .row.user { background: var(--ai-user-bg); color: var(--ai-user-fg); margin-left: auto; }
                                                .row.agent { background: var(--ai-bubble-agent-bg); }
                                                #ai-chat-form { display: flex; gap: 8px; padding: 12px 16px; border-top: 1px solid var(--ai-border-color); }
                                                #ai-chat-input { flex: 1; padding: 10px 12px; border: 1px solid var(--ai-input-border-color); border-radius: var(--ai-radius-control); background: var(--ai-input-bg); color: var(--ai-fg); font-size: 14px; }
                                                #ai-chat-form button { padding: 10px 16px; border: 0; border-radius: var(--ai-radius-control); background: var(--ai-user-bg); color: var(--ai-user-fg); cursor: pointer; }
                                              """;

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
                                             __BASE_CSS__
                                             </style>
                                             <style id="raven-custom">__CUSTOM_CSS__</style>
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

                                             // Prior turns for this conversation, server-rendered on load; empty for a fresh link.
                                             for (const turn of __HISTORY__) addRow(turn.role, turn.text);

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
                                                 // 410 = expired/revoked; 404 = link already swept/unknown. Same UX either way.
                                                 if (resp.status === 410 || resp.status === 404) { agentRow.textContent = "[this link is no longer active]"; return; }
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

    // Inert mirror of the live page for the dashboard customization preview: same
    // head + base styles, sample bubbles, an empty <style id="raven-custom"> slot the
    // dashboard fills as the operator types, and no token/chat script (the preview
    // never talks to the server). __BASE_CSS__ is the only substitution besides the title.
    private const string PreviewHtmlTemplate = """
                                               <!DOCTYPE html>
                                               <html lang="en">
                                               <head>
                                               <meta charset="utf-8">
                                               <meta name="viewport" content="width=device-width, initial-scale=1">
                                               <title>__TITLE__</title>
                                               <style>
                                               __BASE_CSS__
                                               </style>
                                               <style id="raven-custom"></style>
                                               </head>
                                               <body>
                                               <div id="ai-chat">
                                                 <div id="ai-chat-header">__TITLE__</div>
                                                 <div id="ai-chat-feed">
                                                   <div class="row agent">Hi! I'm your AI assistant. How can I help you today?</div>
                                                   <div class="row user">What can you do?</div>
                                                   <div class="row agent">I can answer questions about your data and help you get things done — just ask.</div>
                                                 </div>
                                                 <form id="ai-chat-form" onsubmit="return false">
                                                   <input id="ai-chat-input" autocomplete="off" placeholder="Ask a question..." aria-label="Ask a question">
                                                   <button type="submit">Send</button>
                                                 </form>
                                               </div>
                                               </body>
                                               </html>
                                               """;
}
