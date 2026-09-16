using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

/// A web-widget channel's own theme plus the app default it falls back to. `Theme` is null when the channel
/// follows the app default; the dashboard renders that as its third state.
public sealed record WidgetThemeResponse(
    WidgetTheme? Theme,
    WidgetTheme DefaultTheme,
    IReadOnlyList<WidgetFontOption> FontOptions);

/// The app-level default applied to every web widget that makes no choice of its own.
public sealed record WidgetDefaultThemeResponse(WidgetTheme Theme, IReadOnlyList<WidgetFontOption> FontOptions);

/// A null theme on the channel route clears the channel's choice so it follows the app default; on the
/// app-default route it resets to the built-in default.
public sealed record UpdateWidgetThemeRequest(WidgetTheme? Theme);
