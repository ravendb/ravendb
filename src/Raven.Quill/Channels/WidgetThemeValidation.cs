using System.Text.RegularExpressions;

namespace Raven.Quill.Channels;

/// Validates an operator-supplied theme. Every field is an enum, a bounded number, or text matched against
/// a positive pattern — except <see cref="WidgetTheme.CustomCss"/>, which is deliberately freeform: it is
/// emitted inside a nonce'd style tag under the embed page's CSP, so the only sequence that must never
/// appear is the one that could close that tag.
public static partial class WidgetThemeValidation
{
    // Headroom, not house style. What reads well is the operator's call: the widget truncates the header
    // title and subtitle in CSS and wraps everything else, so none of these lengths is a layout rule.
    // They are only here because the whole theme is inlined into every embed page, and even taken together
    // at their ceilings they come to a few KB - noise beside the logo. Widen them freely.
    public const int MaxSuggestedPrompts = 10;
    public const int MaxSuggestedPromptLength = 200;

    /// Deliberately under `ChannelsEndpoints.MaxDisplayNameLength`, which is what
    /// <see cref="WidgetThemeResolution"/> substitutes for an unset title - keeping the two apart is what
    /// keeps that substitution's clamp exercised rather than dead.
    public const int MaxHeaderTitleLength = 120;

    public const int MaxHeaderSubtitleLength = 200;
    public const int MaxGreetingTitleLength = 160;
    public const int MaxGreetingBodyLength = 1_000;
    public const int MaxInputPlaceholderLength = 160;
    public const int MaxDisclaimerLength = 600;

    // Not headroom: these two are interpolated into a stylesheet and shipped to every visitor, so their
    // bounds are part of the mechanism rather than a matter of taste.
    public const int MaxFontFamilyLength = 200;
    public const int MaxCustomCssLength = 10_000;

    /// ~110KB of base64, comfortably enough for a 128px logo while keeping the theme document small.
    public const int MaxLogoLength = 150_000;

    public const double MinCustomFontSizeRem = 0.625;
    public const double MaxCustomFontSizeRem = 1.5;

    /// The one sequence that could break out of the shell's `<style>` tag; HTML parses style content up to
    /// the literal close tag, so blocking it is sufficient regardless of what surrounds it.
    private const string StyleCloseSequence = "</style";

    [GeneratedRegex("^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")]
    private static partial Regex HexColorPattern { get; }

    /// Raster formats only: an SVG data URI can carry script, and nothing here needs vectors.
    [GeneratedRegex("^data:image/(?:png|jpeg|webp);base64,[A-Za-z0-9+/=]+$")]
    private static partial Regex LogoDataUriPattern { get; }

    /// A custom stack may only use what a font stack actually needs. That rules out `;`, `{`, `}`, `@` and
    /// `url(` by construction rather than by listing them.
    [GeneratedRegex("""^[A-Za-z0-9 ,'"\-]+$""")]
    private static partial Regex FontStackPattern { get; }

    public static bool TryValidate(WidgetTheme? theme, out string? error)
    {
        if (theme is null)
        {
            error = "theme is required";
            return false;
        }

        if (Enum.IsDefined(theme.Appearance) == false)
        {
            error = "appearance must be 'Light', 'Dark' or 'System'";
            return false;
        }

        if (Enum.IsDefined(theme.Radius) == false)
        {
            error = "radius must be 'None', 'Small', 'Medium' or 'Large'";
            return false;
        }

        if (Enum.IsDefined(theme.LogoRadius) == false)
        {
            error = "logoRadius must be 'None', 'Small', 'Medium', 'Large' or 'Pill'";
            return false;
        }

        if (TryValidateColors(theme.Light, "light", out error) == false)
            return false;

        if (TryValidateColors(theme.Dark, "dark", out error) == false)
            return false;

        if (TryValidateFontFamily(theme.FontFamily, out error) == false)
            return false;

        if (TryValidateFontSize(theme.FontSize, theme.CustomFontSizeRem, out error) == false)
            return false;

        if (theme.Logo is not null)
        {
            if (theme.Logo.Length > MaxLogoLength)
            {
                error = $"logo must be {MaxLogoLength} characters or fewer";
                return false;
            }

            if (LogoDataUriPattern.IsMatch(theme.Logo) == false)
            {
                error = "logo must be a base64 data URI of a png, jpeg or webp image";
                return false;
            }
        }

        // A hidden header shows no title, so requiring one would only force the operator to invent a string
        // no visitor ever sees. The length still applies: the value is kept for when the header comes back.
        var headerTitleValid = theme.ShowHeader
            ? TryValidateRequiredText(theme.HeaderTitle, "headerTitle", MaxHeaderTitleLength, out error)
            : TryValidateOptionalText(theme.HeaderTitle, "headerTitle", MaxHeaderTitleLength, out error);

        if (headerTitleValid == false)
            return false;

        if (TryValidateRequiredText(theme.InputPlaceholder, "inputPlaceholder", MaxInputPlaceholderLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.HeaderSubtitle, "headerSubtitle", MaxHeaderSubtitleLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.GreetingTitle, "greetingTitle", MaxGreetingTitleLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.GreetingBody, "greetingBody", MaxGreetingBodyLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.Disclaimer, "disclaimer", MaxDisclaimerLength, out error) == false)
            return false;

        if (TryValidateCustomCss(theme.CustomCss, out error) == false)
            return false;

        return TryValidateSuggestedPrompts(theme.SuggestedPrompts, out error);
    }

    /// Trims and drops blank optional fields, so the validated (and then persisted) document is the canonical
    /// form of whatever the operator typed. Runs *before* validation, which is why it tolerates nulls and
    /// deliberately does not clamp the prompt list - silently dropping a fifth prompt would hide an error the
    /// operator should see.
    public static WidgetTheme Normalize(WidgetTheme theme) => theme with
    {
        Light = NormalizeColors(theme.Light),
        Dark = NormalizeColors(theme.Dark),
        FontFamily = theme.FontFamily?.Trim() ?? "",
        CustomFontSizeRem = theme.FontSize == WidgetFontSize.Custom ? theme.CustomFontSizeRem : null,
        Logo = Blank(theme.Logo),
        HeaderTitle = theme.HeaderTitle?.Trim() ?? "",
        HeaderSubtitle = Blank(theme.HeaderSubtitle),
        GreetingTitle = Blank(theme.GreetingTitle),
        GreetingBody = Blank(theme.GreetingBody),
        InputPlaceholder = theme.InputPlaceholder?.Trim() ?? "",
        Disclaimer = Blank(theme.Disclaimer),
        CustomCss = Blank(theme.CustomCss),
        SuggestedPrompts = (theme.SuggestedPrompts ?? [])
            .Select(prompt => prompt?.Trim() ?? "")
            .Where(prompt => prompt.Length > 0)
            .ToArray(),
    };

    private static string? Blank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static WidgetThemeColors NormalizeColors(WidgetThemeColors? colors) => new(
        ButtonColor: colors?.ButtonColor?.Trim().ToLowerInvariant() ?? "",
        MessageColor: colors?.MessageColor?.Trim().ToLowerInvariant() ?? "",
        BackgroundColor: colors?.BackgroundColor?.Trim().ToLowerInvariant() ?? "");

    private static bool TryValidateColors(WidgetThemeColors? colors, string scheme, out string? error)
    {
        if (colors is null)
        {
            error = $"{scheme} colors are required";
            return false;
        }

        if (HexColorPattern.IsMatch(colors.ButtonColor ?? "") == false)
        {
            error = $"{scheme}.buttonColor must be a hex color such as '#2f6f4f'";
            return false;
        }

        if (HexColorPattern.IsMatch(colors.MessageColor ?? "") == false)
        {
            error = $"{scheme}.messageColor must be a hex color such as '#ffefec'";
            return false;
        }

        if (HexColorPattern.IsMatch(colors.BackgroundColor ?? "") == false)
        {
            error = $"{scheme}.backgroundColor must be a hex color such as '#ffffff'";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryValidateFontSize(WidgetFontSize fontSize, double? customRem, out string? error)
    {
        if (Enum.IsDefined(fontSize) == false)
        {
            error = "fontSize must be 'Small', 'Medium', 'Large' or 'Custom'";
            return false;
        }

        if (fontSize == WidgetFontSize.Custom)
        {
            if (customRem is null || double.IsFinite(customRem.Value) == false)
            {
                error = "customFontSizeRem is required when fontSize is 'Custom'";
                return false;
            }

            if (customRem.Value < MinCustomFontSizeRem || customRem.Value > MaxCustomFontSizeRem)
            {
                error = $"customFontSizeRem must be between {MinCustomFontSizeRem} and {MaxCustomFontSizeRem}";
                return false;
            }
        }

        error = null;
        return true;
    }

    private static bool TryValidateFontFamily(string? fontFamily, out string? error)
    {
        var stack = fontFamily?.Trim() ?? "";
        if (stack.Length == 0)
        {
            error = "fontFamily is required";
            return false;
        }

        // A curated stack is accepted verbatim; only a hand-written one has to clear the character check.
        if (WidgetFonts.IsCurated(stack))
        {
            error = null;
            return true;
        }

        if (stack.Length > MaxFontFamilyLength)
        {
            error = $"fontFamily must be {MaxFontFamilyLength} characters or fewer";
            return false;
        }

        if (FontStackPattern.IsMatch(stack) == false)
        {
            error = "fontFamily may only contain letters, digits, spaces, commas, hyphens and quotes";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryValidateCustomCss(string? customCss, out string? error)
    {
        if (customCss is null)
        {
            error = null;
            return true;
        }

        if (customCss.Length > MaxCustomCssLength)
        {
            error = $"customCss must be {MaxCustomCssLength} characters or fewer";
            return false;
        }

        if (customCss.Contains(StyleCloseSequence, StringComparison.OrdinalIgnoreCase))
        {
            error = $"customCss may not contain '{StyleCloseSequence}'";
            return false;
        }

        error = null;
        return true;
    }

    private static bool TryValidateRequiredText(string? value, string field, int maxLength, out string? error)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            error = $"{field} is required";
            return false;
        }

        return TryValidateOptionalText(value, field, maxLength, out error);
    }

    private static bool TryValidateOptionalText(string? value, string field, int maxLength, out string? error)
    {
        if (value is not null && value.Trim().Length > maxLength)
        {
            error = $"{field} must be {maxLength} characters or fewer";
            return false;
        }

        error = null;
        return true;
    }

    /// Not optional the way a nullable string is: the widget's own theme type declares a plain array, and a
    /// null - in the list or instead of it - would reach the welcome screen as one and throw there.
    private static bool TryValidateSuggestedPrompts(string[]? prompts, out string? error)
    {
        if (prompts is null)
        {
            error = "suggestedPrompts is required; use an empty list for none";
            return false;
        }

        if (prompts.Length > MaxSuggestedPrompts)
        {
            error = $"suggestedPrompts must contain {MaxSuggestedPrompts} entries or fewer";
            return false;
        }

        foreach (var prompt in prompts)
        {
            if (prompt is null)
            {
                error = "suggestedPrompts may not contain a null entry";
                return false;
            }

            if (prompt.Trim().Length > MaxSuggestedPromptLength)
            {
                error = $"each suggested prompt must be {MaxSuggestedPromptLength} characters or fewer";
                return false;
            }
        }

        error = null;
        return true;
    }
}
