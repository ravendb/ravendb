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

public static class EmbedEndpoints
{
    public const string ChatRateLimitPolicy = "embed-chat";

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

        var csp = BaseCsp;
        if (channel.AllowedOrigins.Length > 0)
        {
            var dashboardOrigin = $"{ctx.Request.Scheme}://{ApplianceHost.WithSubdomain(ctx.Request.Host, "dashboard").ToUriComponent()}";
            var head = Array.IndexOf(channel.AllowedOrigins, dashboardOrigin) >= 0 ? "'self'" : $"'self' {dashboardOrigin}";
            csp += $"; frame-ancestors {head} {string.Join(' ', channel.AllowedOrigins)}";
        }

        ctx.Response.Headers["Content-Security-Policy"] = csp;

        ctx.Response.Headers["Referrer-Policy"] = "no-referrer";

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

        if (IsOriginAllowed(ctx.Request, channel.AllowedOrigins) == false)
        {
            ctx.Response.StatusCode = StatusCodes.Status403Forbidden;
            await ctx.Response.WriteAsJsonAsync(new ApiErrorResponse("origin not allowed", Code: "origin_forbidden"), ct);
            return;
        }

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

        var config = await AgentLookup.FindAsync(store, app.Database, channel.AgentId, ct);
        if (config is null)
        {
            await RefundInvocationAsync(store, app.Database, token, logger);
            ctx.Response.StatusCode = StatusCodes.Status404NotFound;
            return;
        }

        NdjsonStream.SetHeaders(ctx);
        var streamedAny = false;
        try
        {
            var result = await router.RunAsync(
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
        }
        catch (Exception e)
        {
            logger.LogError(e, "embed chat failed for tokenPrefix={TokenPrefix}", EmbedLink.RedactToken(token));

            if (streamedAny == false)
                await RefundInvocationAsync(store, app.Database, token, logger);

            try
            {
                await NdjsonStream.WriteLineAsync(ctx, new { type = "error", message = "Chat failed. See server logs for details." });
            }
            catch
            {
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
            }
        }

        return InvocationGate.Exhausted;
    }

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
                }
            }
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "failed to refund invocation for tokenPrefix={TokenPrefix}", EmbedLink.RedactToken(token));
        }
    }

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

        if (channel.Enabled == false || link.Revoked || link.ExpiresAt <= DateTime.UtcNow)
        {
            ctx.Response.StatusCode = StatusCodes.Status410Gone;
            return null;
        }

        return resolved;
    }

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

            if (resolveStyle && channel is { Type: ChannelType.IFrame })
            {
                var defaults = IFrameStyleResolution.OwnStyle(channel) is null
                    ? await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct)
                    : null;
                style = IFrameStyleResolution.ForChannel(channel, defaults);
            }
        }

        if (channel is null || channel.Type != ChannelType.IFrame)
            return null;

        return (app, link, channel, style);
    }

    private static string BuildEmbedHtml(string token, string displayName, ResolvedIFrameStyle style, string historyJson)
    {
        var title = WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(displayName) ? "AI Assistant" : displayName);
        return EmbedHtmlTemplate
            .Replace("__TOKEN__", token)
            .Replace("__BASE_CSS__", BuildWidgetBaseCss(style.Style))
            .Replace("__CUSTOM_CSS__", IFrameCss.Sanitize(style.CustomCss))
            .Replace("__TITLE__", title)
            .Replace("__HISTORY__", historyJson);
    }

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

    internal static string BuildPreviewHtml(string? displayName)
    {
        var title = WebUtility.HtmlEncode(string.IsNullOrWhiteSpace(displayName) ? "AI Assistant" : displayName);
        return PreviewHtmlTemplate
            .Replace("__TITLE__", title)
            .Replace("__BASE_CSS__", WidgetBaseCss);
    }

    internal sealed class EmbedLogger;

    internal static string BuildWidgetBaseCss(IFrameStyle style) =>
        IFrameStyleVariables.BuildRootBlock(style) + "\n" + WidgetBaseCssRules;

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
