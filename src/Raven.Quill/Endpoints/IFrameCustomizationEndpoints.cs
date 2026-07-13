using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

/// <summary>
/// Operator-facing customization of the web-widget (iFrame) embed surface: a channel's own
/// CSS, the app-level default applied to channels with none, and the inert preview document
/// the dashboard live-styles. CSS styling is iFrame-only — Telegram/WhatsApp render in their
/// own apps — so these live in their own <c>iframe</c> group rather than the type-agnostic
/// <see cref="ChannelsEndpoints"/>. The public embed page that consumes the stored CSS is
/// <see cref="EmbedEndpoints"/>.
/// </summary>
public static class IFrameCustomizationEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}/iframe").WithTags("iframe").RequireAuthorization();

        group.MapGet("/{widgetId}/customization", GetCustomizationAsync)
            .WithName("iframe.getCustomization")
            .WithDescription("Returns a web-widget channel's own embed CSS plus the app default, for the styling editor.")
            .Produces<IFrameCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/{widgetId}/customization", UpdateCustomizationAsync)
            .WithName("iframe.updateCustomization")
            .WithDescription("Saves a web-widget channel's embed CSS. An empty body clears it so the channel falls back to the app default.")
            .Accepts<UpdateIFrameCustomizationRequest>("application/json")
            .Produces<IFrameCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/default-customization", GetDefaultCustomizationAsync)
            .WithName("iframe.getDefaultCustomization")
            .WithDescription("Returns the app-level default web-widget embed CSS applied to channels with no CSS of their own.")
            .Produces<IFrameDefaultCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/default-customization", UpdateDefaultCustomizationAsync)
            .WithName("iframe.updateDefaultCustomization")
            .WithDescription("Saves the app-level default web-widget embed CSS. An empty body clears it.")
            .Accepts<UpdateIFrameCustomizationRequest>("application/json")
            .Produces<IFrameDefaultCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/preview", GetPreviewAsync)
            .WithName("iframe.preview")
            .WithDescription("Returns the inert web-widget preview document (base styles + sample bubbles) the dashboard frames to live-preview CSS edits.")
            .Produces<IFramePreviewResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/style-guide", GetStyleGuideAsync)
            .WithName("iframe.getStyleGuide")
            .WithDescription("Returns the web-widget embed page's base CSS, used as the styling editor's starter template and \"reset to default\" content.")
            .Produces<IFrameStyleGuideResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetCustomizationAsync(
        string slug,
        string widgetId,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await LoadIFrameChannelAsync(session, widgetId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no iFrame channel '{widgetId}' in app '{slug}'"));

        var defaults = await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct);
        return Results.Ok(new IFrameCustomizationResponse(channel.CustomCss, defaults?.Css));
    }

    private static async Task<IResult> UpdateCustomizationAsync(
        string slug,
        string widgetId,
        UpdateIFrameCustomizationRequest body,
        IDocumentStore store,
        ILogger<IFrameCustomizationLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));
        if (IFrameCss.TryValidate(body.Css, out var cssError) == false)
            return Results.BadRequest(new ApiErrorResponse(cssError!));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await LoadIFrameChannelAsync(session, widgetId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no iFrame channel '{widgetId}' in app '{slug}'"));

        // Collapse empty to null so "uses the app default" has a single representation.
        channel.CustomCss = string.IsNullOrWhiteSpace(body.Css) ? null : body.Css;
        await session.SaveChangesAsync(ct);

        var defaults = await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct);
        logger.LogInformation(
            "Updated iFrame customization slug={Slug} widgetId={WidgetId} hasCss={HasCss}",
            app.Slug, widgetId, channel.CustomCss is not null);
        return Results.Ok(new IFrameCustomizationResponse(channel.CustomCss, defaults?.Css));
    }

    private static async Task<IResult> GetDefaultCustomizationAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var defaults = await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct);
        return Results.Ok(new IFrameDefaultCustomizationResponse(defaults?.Css));
    }

    private static async Task<IResult> UpdateDefaultCustomizationAsync(
        string slug,
        UpdateIFrameCustomizationRequest body,
        IDocumentStore store,
        ILogger<IFrameCustomizationLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));
        if (IFrameCss.TryValidate(body.Css, out var cssError) == false)
            return Results.BadRequest(new ApiErrorResponse(cssError!));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        var css = string.IsNullOrWhiteSpace(body.Css) ? null : body.Css;

        using var session = store.OpenAsyncSession(app.Database);
        var defaults = await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct);
        if (defaults is null)
        {
            defaults = new IFrameStyleDefaults { Id = IFrameStyleDefaults.DocumentId };
            await session.StoreAsync(defaults, ct);
        }

        defaults.Css = css;
        defaults.UpdatedAt = DateTime.UtcNow;
        await session.SaveChangesAsync(ct);

        logger.LogInformation("Updated iFrame default customization slug={Slug} hasCss={HasCss}", app.Slug, css is not null);
        return Results.Ok(new IFrameDefaultCustomizationResponse(defaults.Css));
    }

    private static async Task<IResult> GetPreviewAsync(
        string slug,
        string? title,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        return Results.Ok(new IFramePreviewResponse(EmbedEndpoints.BuildPreviewHtml(title)));
    }

    private static async Task<IResult> GetStyleGuideAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        return Results.Ok(new IFrameStyleGuideResponse(EmbedEndpoints.WidgetBaseCss));
    }

    /// <summary>Loads a channel by widgetId, returning null unless it exists and is an iFrame
    /// channel — customization is iFrame-only, so a non-iFrame id collapses to 404.</summary>
    private static async Task<Channel?> LoadIFrameChannelAsync(
        IAsyncDocumentSession session, string widgetId, CancellationToken ct)
    {
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + widgetId, ct);
        return channel is { Type: ChannelType.IFrame } ? channel : null;
    }

    /// Logger category marker — keeps the ILogger generic-arg out of the public surface.
    internal sealed class IFrameCustomizationLogger;
}
