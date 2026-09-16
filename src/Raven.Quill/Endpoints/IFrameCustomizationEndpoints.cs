using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Logging;

namespace Raven.Quill.Endpoints;

public static class IFrameCustomizationEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/apps/{slug}/iframe").WithTags("iframe").RequireAuthorization();

        group.MapGet("/{channelId}/theme", GetThemeAsync)
            .WithName("iframe.getTheme")
            .WithDescription("Returns a web-widget channel's own theme plus the resolved app default, for the theme editor. A null theme means the channel follows the app default.")
            .Produces<WidgetThemeResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/{channelId}/theme", UpdateThemeAsync)
            .WithName("iframe.updateTheme")
            .WithDescription("Saves a web-widget channel's theme. A null theme clears the channel's choice so it follows the app default.")
            .Accepts<UpdateWidgetThemeRequest>("application/json")
            .Produces<WidgetThemeResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapGet("/default-theme", GetDefaultThemeAsync)
            .WithName("iframe.getDefaultTheme")
            .WithDescription("Returns the app-level default web-widget theme applied to channels that make no choice of their own.")
            .Produces<WidgetDefaultThemeResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);

        group.MapPut("/default-theme", UpdateDefaultThemeAsync)
            .WithName("iframe.updateDefaultTheme")
            .WithDescription("Saves the app-level default web-widget theme. A null theme resets it to the built-in default.")
            .Accepts<UpdateWidgetThemeRequest>("application/json")
            .Produces<WidgetDefaultThemeResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status404NotFound);
    }

    private static async Task<IResult> GetThemeAsync(
        string slug,
        string channelId,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await LoadIFrameChannelAsync(session, channelId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no iFrame channel '{channelId}' in app '{slug}'"));

        var defaults = await session.LoadAsync<WidgetThemeDefaults>(WidgetThemeDefaults.DocumentId, ct);
        return Results.Ok(BuildThemeResponse(channel, defaults));
    }

    private static async Task<IResult> UpdateThemeAsync(
        string slug,
        string channelId,
        UpdateWidgetThemeRequest body,
        IDocumentStore store,
        QuillLogger<IFrameCustomizationLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        if (TryNormalizeTheme(body.Theme, out var theme, out var error) == false)
            return Results.BadRequest(new ApiErrorResponse(error!));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var channel = await LoadIFrameChannelAsync(session, channelId, ct);
        if (channel is null)
            return Results.NotFound(new ApiErrorResponse($"no iFrame channel '{channelId}' in app '{slug}'"));

        channel.Theme = theme;
        await session.SaveChangesAsync(ct);

        var defaults = await session.LoadAsync<WidgetThemeDefaults>(WidgetThemeDefaults.DocumentId, ct);
        if (logger.IsInfoEnabled)
            logger.Info(
                $"Updated web widget theme slug={app.Slug} channelId={channelId} follows={theme is null}");
        return Results.Ok(BuildThemeResponse(channel, defaults));
    }

    private static async Task<IResult> GetDefaultThemeAsync(
        string slug,
        IDocumentStore store,
        CancellationToken ct)
    {
        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var defaults = await session.LoadAsync<WidgetThemeDefaults>(WidgetThemeDefaults.DocumentId, ct);
        return Results.Ok(new WidgetDefaultThemeResponse(WidgetThemeResolution.ForDefaults(defaults), WidgetFonts.Curated));
    }

    private static async Task<IResult> UpdateDefaultThemeAsync(
        string slug,
        UpdateWidgetThemeRequest body,
        IDocumentStore store,
        QuillLogger<IFrameCustomizationLogger> logger,
        CancellationToken ct)
    {
        if (body is null)
            return Results.BadRequest(new ApiErrorResponse("request body is required"));

        if (TryNormalizeTheme(body.Theme, out var theme, out var error) == false)
            return Results.BadRequest(new ApiErrorResponse(error!));

        var app = await AppLookup.LoadAppAsync(store, slug, ct);
        if (app is null)
            return Results.NotFound(new ApiErrorResponse($"no app with slug '{slug}'"));

        using var session = store.OpenAsyncSession(app.Database);
        var defaults = await session.LoadAsync<WidgetThemeDefaults>(WidgetThemeDefaults.DocumentId, ct);
        if (defaults is null)
        {
            defaults = new WidgetThemeDefaults { Id = WidgetThemeDefaults.DocumentId };
            await session.StoreAsync(defaults, ct);
        }

        // A cleared app default means "back to the built-in", which is stored explicitly so the resolved
        // default never depends on which fields the built-in happened to have when the document was written.
        defaults.Theme = theme ?? WidgetTheme.Default;
        defaults.UpdatedAt = DateTime.UtcNow;
        await session.SaveChangesAsync(ct);

        if (logger.IsInfoEnabled)
            logger.Info($"Updated default web widget theme slug={app.Slug}");
        return Results.Ok(new WidgetDefaultThemeResponse(defaults.Theme, WidgetFonts.Curated));
    }

    private static bool TryNormalizeTheme(WidgetTheme? theme, out WidgetTheme? normalized, out string? error)
    {
        if (theme is null)
        {
            normalized = null;
            error = null;
            return true;
        }

        // Normalize first so a trimmed value is what gets validated: "  #2F6F4F " is a legitimate accent.
        var candidate = WidgetThemeValidation.Normalize(theme);
        if (WidgetThemeValidation.TryValidate(candidate, out error) == false)
        {
            normalized = null;
            return false;
        }

        normalized = candidate;
        return true;
    }

    private static WidgetThemeResponse BuildThemeResponse(Channel channel, WidgetThemeDefaults? defaults) =>
        new(channel.Theme, WidgetThemeResolution.ForDefaults(defaults), WidgetFonts.Curated);

    private static async Task<Channel?> LoadIFrameChannelAsync(
        IAsyncDocumentSession session, string channelId, CancellationToken ct)
    {
        var channel = await session.LoadAsync<Channel>(Channel.IdPrefix + channelId, ct);
        return channel is { Type: ChannelType.IFrame } ? channel : null;
    }

    internal sealed class IFrameCustomizationLogger;
}
