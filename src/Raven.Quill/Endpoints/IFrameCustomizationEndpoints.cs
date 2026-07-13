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
/// style (a built-in Light/Dark preset or custom CSS), the app-level default applied to
/// channels that make no choice, and the inert preview document the dashboard live-styles.
/// Embed styling is iFrame-only — Telegram/WhatsApp render in their own apps — so these live
/// in their own <c>iframe</c> group rather than the type-agnostic
/// <see cref="ChannelsEndpoints"/>. The public embed page that consumes the stored style is
/// <see cref="EmbedEndpoints"/>.
/// </summary>
public static class IFrameCustomizationEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}/iframe").WithTags("iframe").RequireAuthorization();

        group.MapGet("/{widgetId}/customization", GetCustomizationAsync)
            .WithName("iframe.getCustomization")
            .WithDescription("Returns a web-widget channel's own embed style plus the resolved app default, for the styling editor.")
            .Produces<IFrameCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/{widgetId}/customization", UpdateCustomizationAsync)
            .WithName("iframe.updateCustomization")
            .WithDescription("Saves a web-widget channel's embed style: a built-in preset, custom CSS, or (with a null style) follow the app default.")
            .Accepts<UpdateIFrameCustomizationRequest>("application/json")
            .Produces<IFrameCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/default-customization", GetDefaultCustomizationAsync)
            .WithName("iframe.getDefaultCustomization")
            .WithDescription("Returns the resolved app-level default web-widget embed style applied to channels that make no choice of their own.")
            .Produces<IFrameDefaultCustomizationResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/default-customization", UpdateDefaultCustomizationAsync)
            .WithName("iframe.updateDefaultCustomization")
            .WithDescription("Saves the app-level default web-widget embed style: a built-in preset or custom CSS. A null style resets to the Light preset.")
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
        return Results.Ok(BuildCustomizationResponse(channel, defaults));
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
        if (TryValidateStyle(body, out var styleError) == false)
            return Results.BadRequest(new ApiErrorResponse(styleError!));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await LoadIFrameChannelAsync(session, widgetId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no iFrame channel '{widgetId}' in app '{slug}'"));

        // A null style clears the channel's choice so it follows the app default; CSS is kept
        // only for Custom so "preset" and "custom" can't both linger on the doc.
        channel.Style = body.Style;
        channel.CustomCss = body.Style == IFrameStyle.Custom ? body.Css : null;
        await session.SaveChangesAsync(ct);

        var defaults = await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct);
        logger.LogInformation(
            "Updated iFrame customization slug={Slug} widgetId={WidgetId} style={Style}",
            app.Slug, widgetId, channel.Style?.ToString() ?? "(app default)");
        return Results.Ok(BuildCustomizationResponse(channel, defaults));
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
        var resolved = IFrameStyleResolution.ForDefaults(defaults);
        return Results.Ok(new IFrameDefaultCustomizationResponse(resolved.Style, resolved.CustomCss));
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
        if (TryValidateStyle(body, out var styleError) == false)
            return Results.BadRequest(new ApiErrorResponse(styleError!));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        // The app default always resolves to something — a null style means the factory Light preset.
        var style = body.Style ?? IFrameStyle.Light;

        using var session = store.OpenAsyncSession(app.Database);
        var defaults = await session.LoadAsync<IFrameStyleDefaults>(IFrameStyleDefaults.DocumentId, ct);
        if (defaults is null)
        {
            defaults = new IFrameStyleDefaults { Id = IFrameStyleDefaults.DocumentId };
            await session.StoreAsync(defaults, ct);
        }

        defaults.Style = style;
        defaults.Css = style == IFrameStyle.Custom ? body.Css : null;
        defaults.UpdatedAt = DateTime.UtcNow;
        await session.SaveChangesAsync(ct);

        logger.LogInformation("Updated iFrame default customization slug={Slug} style={Style}", app.Slug, style);
        return Results.Ok(new IFrameDefaultCustomizationResponse(style, defaults.Css));
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

        return Results.Ok(new IFrameStyleGuideResponse(
            EmbedEndpoints.WidgetBaseCss,
            IFrameStyleVariables.BuildRootBlock(IFrameStyle.Light),
            IFrameStyleVariables.BuildRootBlock(IFrameStyle.Dark)));
    }

    /// <summary>Validates the shared update body: CSS is required and validated only for the
    /// Custom style — presets carry their own styling, so any CSS sent with them is ignored.</summary>
    private static bool TryValidateStyle(UpdateIFrameCustomizationRequest body, out string? error)
    {
        if (body.Style != IFrameStyle.Custom)
        {
            error = null;
            return true;
        }

        if (string.IsNullOrWhiteSpace(body.Css))
        {
            error = "css is required when style is 'Custom'";
            return false;
        }

        return IFrameCss.TryValidate(body.Css, out error);
    }

    /// <summary>Builds the per-channel editor payload: the channel's own choice (with the
    /// legacy CSS-only fallback applied) plus the resolved app default it would inherit.</summary>
    private static IFrameCustomizationResponse BuildCustomizationResponse(Channel channel, IFrameStyleDefaults? defaults)
    {
        var resolvedDefault = IFrameStyleResolution.ForDefaults(defaults);
        return new IFrameCustomizationResponse(
            IFrameStyleResolution.OwnStyle(channel),
            channel.CustomCss,
            resolvedDefault.Style,
            resolvedDefault.CustomCss);
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
