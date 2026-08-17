using System.Linq;
using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Embed;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Metrics;
using Raven.Quill.Wizard;

namespace Raven.Quill.Endpoints;

public static class EmbedEndpoints
{
    public const string ChatRateLimitPolicy = "embed-chat";

    public static void Map(WebApplication app)
    {
        app.MapGet("/apps/{slug}/embed/{token}", ServeEmbedPageAsync)
            .WithName("embed.page")
            .ExcludeFromDescription();

        app.MapPost("/apps/{slug}/embed/{token}/chat", StreamEmbedChatAsync)
            .WithName("embed.chat")
            .Accepts<EmbedChatRequest>("application/json")
            .RequireRateLimiting(ChatRateLimitPolicy)
            .ExcludeFromDescription();
    }

    private static async Task ServeEmbedPageAsync(
        string slug,
        string token,
        IDocumentStore store,
        WidgetAssets assets,
        ILogger<EmbedLogger> logger,
        HttpContext ctx)
    {
        var ct = ctx.RequestAborted;

        var (status, resolved) = await ResolveLiveLinkAsync(store, slug, token, resolveTheme: true, ct);
        if (status != LinkStatus.Ok)
        {
            await WriteNoticeAsync(ctx, resolved?.theme ?? WidgetTheme.Default,
                status == LinkStatus.Gone ? StatusCodes.Status410Gone : StatusCodes.Status404NotFound,
                status == LinkStatus.Gone ? WidgetNotice.Notice.Expired("expired") : WidgetNotice.Notice.NotFound(),
                ct);
            return;
        }

        var (app, link, channel, theme) = resolved!.Value;

        // A host page may pin the scheme per visitor without a theme of its own: ?appearance=dark|light|system.
        // Only the appearance is overridable this way - it picks between palettes the operator already chose.
        if (TryParseAppearance(ctx.Request.Query["appearance"], out var appearanceOverride))
            theme = theme with { Appearance = appearanceOverride };

        if (assets.IsAvailable == false)
        {
            await WriteNoticeAsync(ctx, theme, StatusCodes.Status503ServiceUnavailable,
                WidgetNotice.Notice.Unavailable(), ct);
            return;
        }

        var nonce = WidgetShell.CreateNonce();
        ctx.Response.Headers["Content-Security-Policy"] = WidgetShell.BuildCsp(nonce, FrameAncestors(ctx, channel));

        // keep the bearer token out of cross-origin referer logs
        ctx.Response.Headers["Referrer-Policy"] = "no-referrer";
        ctx.Response.Headers["X-Content-Type-Options"] = "nosniff";

        // the shell embeds the link's bearer token, so it must never sit in a shared cache
        ctx.Response.Headers.CacheControl = "no-store";

        var agent = await AgentLookup.FindAsync(store, app.Database, channel.AgentId, ct);
        var replyField = AgentOutputShape.ResolveReplyField(agent);

        var serializerOptions = ctx.RequestServices
            .GetRequiredService<IOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>>().Value.SerializerOptions;

        var history = await BuildHistoryAsync(store, app.Database, link.ConversationId, replyField, logger, ct);
        var configJson = WidgetShell.SerializeConfig(
            new EmbedWidgetConfig("live", $"/apps/{app.Slug}/embed/{token}/chat", theme, history),
            serializerOptions);

        ctx.Response.ContentType = "text/html; charset=utf-8";
        await ctx.Response.WriteAsync(
            WidgetShell.BuildHtml(assets, nonce, theme.HeaderTitle, theme, configJson), ct);
    }

    private static bool TryParseAppearance(string? value, out WidgetAppearance appearance)
    {
        appearance = default;
        if (string.IsNullOrEmpty(value))
            return false;

        return Enum.TryParse(value, ignoreCase: true, out appearance) && Enum.IsDefined(appearance);
    }

    /// A channel with no configured origins is the operator's explicit opt-in to open embedding (the create
    /// endpoint rejects an omitted list precisely so the choice is deliberate), so no directive is emitted
    /// and any site may frame the page. Once origins exist the page is restricted to them, plus the
    /// appliance itself and the dashboard for the operator's own preview.
    private static string[] FrameAncestors(HttpContext ctx, Channel channel)
    {
        if (channel.AllowedOrigins.Length == 0)
            return [];

        var dashboardOrigin =
            $"{ctx.Request.Scheme}://{ApplianceHost.WithSubdomain(ctx.Request.Host, "dashboard").ToUriComponent()}";

        var ancestors = new List<string> { "'self'" };
        if (Array.IndexOf(channel.AllowedOrigins, dashboardOrigin) < 0)
            ancestors.Add(dashboardOrigin);
        ancestors.AddRange(channel.AllowedOrigins);
        return ancestors.ToArray();
    }

    private static async Task WriteNoticeAsync(
        HttpContext ctx, WidgetTheme theme, int statusCode, WidgetNotice.Notice notice, CancellationToken ct)
    {
        var nonce = WidgetShell.CreateNonce();

        ctx.Response.StatusCode = statusCode;
        ctx.Response.Headers["Content-Security-Policy"] = WidgetNotice.BuildCsp(nonce);
        ctx.Response.Headers["Referrer-Policy"] = "no-referrer";
        ctx.Response.Headers["X-Content-Type-Options"] = "nosniff";
        ctx.Response.Headers.CacheControl = "no-store";
        ctx.Response.ContentType = "text/html; charset=utf-8";

        await ctx.Response.WriteAsync(WidgetNotice.BuildHtml(theme, nonce, notice), ct);
    }

    private static async Task StreamEmbedChatAsync(
        string slug,
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

        var (status, resolved) = await ResolveLiveLinkAsync(store, slug, token, resolveTheme: false, ct);
        if (status != LinkStatus.Ok)
        {
            // the widget maps both of these onto its own terminal states, so no body is needed
            ctx.Response.StatusCode = status == LinkStatus.Gone
                ? StatusCodes.Status410Gone
                : StatusCodes.Status404NotFound;
            return;
        }

        var (app, link, channel, _) = resolved!.Value;

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
                new AgentRequest(app.Database, config.Identifier, gate.ConversationId, body.Prompt, channel.Id!, link.Parameters),
                async chunk =>
                {
                    streamedAny = true;
                    await NdjsonStream.WriteLineAsync(ctx, new { type = "chunk", text = chunk });
                },
                config,
                ct);

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

            // refund the reserved invocation only if nothing streamed (mid-stream abort stays consumed)
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

    // OCC + retry so concurrent turns can't exceed MaxInvocations
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

        // A same-origin fetch from inside the frame sends no Origin, and neither does a CLI smoke test.
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

    private enum LinkStatus { Ok, NotFound, Gone }

    /// `Gone` still carries the resolved link so the page route can render its notice in the channel's own
    /// theme — an expired session should not look like a different product.
    private static async Task<(LinkStatus Status, (App app, EmbedLink link, Channel channel, WidgetTheme theme)? Link)>
        ResolveLiveLinkAsync(IDocumentStore store, string slug, string token, bool resolveTheme, CancellationToken ct)
    {
        if (EmbedLink.IsWellFormedToken(token) == false || Slugifier.IsWellFormed(slug) == false)
            return (LinkStatus.NotFound, null);

        var resolved = await ResolveAsync(store, slug, token, resolveTheme, ct);
        if (resolved is null)
            return (LinkStatus.NotFound, null);

        var (_, link, channel, _) = resolved.Value;

        return channel.Enabled == false || link.Revoked || link.ExpiresAt <= DateTime.UtcNow
            ? (LinkStatus.Gone, resolved)
            : (LinkStatus.Ok, resolved);
    }

    private static async Task<(App app, EmbedLink link, Channel channel, WidgetTheme theme)?> ResolveAsync(
        IDocumentStore store, string slug, string token, bool resolveTheme, CancellationToken ct)
    {
        App? app;
        using (var cfg = store.OpenAsyncSession())
            app = await cfg.LoadAsync<App>($"apps/{slug}", ct);

        if (app is null)
            return null;

        EmbedLink? link;
        Channel? channel;
        var theme = WidgetTheme.Default;
        using (var session = store.OpenAsyncSession(app.Database))
        {
            link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token, ct);
            if (link is null)
                return null;

            channel = await session.LoadAsync<Channel>(Channel.IdPrefix + link.ChannelId, ct);

            if (resolveTheme && channel is { Type: ChannelType.IFrame })
            {
                var defaults = channel.Theme is null
                    ? await session.LoadAsync<WidgetThemeDefaults>(WidgetThemeDefaults.DocumentId, ct)
                    : null;
                theme = WidgetThemeResolution.ForChannel(channel, defaults);
            }
        }

        if (channel is null || channel.Type != ChannelType.IFrame)
            return null;

        return (app, link, channel, theme);
    }

    private static async Task<HistoryTurn[]> BuildHistoryAsync(
        IDocumentStore store, string database, string? conversationId, string replyField,
        ILogger<EmbedLogger> logger, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(conversationId))
            return [];
        try
        {
            var result = await store.AI.ForDatabase(database).GetConversationMessagesAsync(new GetConversationMessagesOptions
            {
                ConversationId = conversationId,
                DetailLevel = AiConversationDetailLevel.Simple,
            }, ct);
            if (result is null)
                return [];
            return MetricsReadService.MapTranscript(result.Messages, replyField)
                .Select(m => new HistoryTurn(m.Role, m.Content ?? ""))
                .ToArray();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // the page still works without history, but the visitor's transcript is gone - say so in the logs
            logger.LogWarning(e, "failed to load embed history for conversationId={ConversationId}", conversationId);
            return [];
        }
    }

    internal sealed class EmbedLogger;
}
