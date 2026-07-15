using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.Endpoints;

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

    private static IFrameCustomizationResponse BuildCustomizationResponse(Channel channel, IFrameStyleDefaults? defaults)
    {
        var resolvedDefault = IFrameStyleResolution.ForDefaults(defaults);
        return new IFrameCustomizationResponse(
            IFrameStyleResolution.OwnStyle(channel),
            channel.CustomCss,
            resolvedDefault.Style,
            resolvedDefault.CustomCss);
    }

    private static async Task<Channel?> LoadIFrameChannelAsync(
        IAsyncDocumentSession session, string widgetId, CancellationToken ct)
    {
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + widgetId, ct);
        return channel is { Type: ChannelType.IFrame } ? channel : null;
    }

    internal sealed class IFrameCustomizationLogger;
}
