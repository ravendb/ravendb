using System.Text.RegularExpressions;

namespace Raven.Quill.Channels;

/// Validates an operator-supplied theme. Unlike the freeform CSS this replaced, nothing here is a
/// substring blocklist: every field is either an enum, a bounded number, or text matched against a
/// positive pattern, so there is no sequence to smuggle past.
public static partial class WidgetThemeValidation
{
    public const int MaxRadius = 24;
    public const int MaxSuggestedPrompts = 4;
    public const int MaxSuggestedPromptLength = 80;
    public const int MaxHeaderTitleLength = 60;
    public const int MaxHeaderSubtitleLength = 100;
    public const int MaxAvatarInitialsLength = 3;
    public const int MaxGreetingTitleLength = 80;
    public const int MaxGreetingBodyLength = 240;
    public const int MaxInputPlaceholderLength = 80;
    public const int MaxDisclaimerLength = 200;
    public const int MaxFontFamilyLength = 200;

    [GeneratedRegex("^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")]
    private static partial Regex HexColorPattern { get; }

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

        if (Enum.IsDefined(theme.Density) == false)
        {
            error = "density must be 'Comfortable' or 'Compact'";
            return false;
        }

        if (HexColorPattern.IsMatch(theme.AccentColor ?? "") == false)
        {
            error = "accentColor must be a hex colour such as '#2f6f4f'";
            return false;
        }

        if (theme.Radius < 0 || theme.Radius > MaxRadius)
        {
            error = $"radius must be between 0 and {MaxRadius}";
            return false;
        }

        if (TryValidateFontFamily(theme.FontFamily, out error) == false)
            return false;

        if (TryValidateRequiredText(theme.HeaderTitle, "headerTitle", MaxHeaderTitleLength, out error) == false)
            return false;

        if (TryValidateRequiredText(theme.InputPlaceholder, "inputPlaceholder", MaxInputPlaceholderLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.HeaderSubtitle, "headerSubtitle", MaxHeaderSubtitleLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.AvatarInitials, "avatarInitials", MaxAvatarInitialsLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.GreetingTitle, "greetingTitle", MaxGreetingTitleLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.GreetingBody, "greetingBody", MaxGreetingBodyLength, out error) == false)
            return false;

        if (TryValidateOptionalText(theme.Disclaimer, "disclaimer", MaxDisclaimerLength, out error) == false)
            return false;

        return TryValidateSuggestedPrompts(theme.SuggestedPrompts, out error);
    }

    /// Trims and drops blank optional fields, so the validated (and then persisted) document is the canonical
    /// form of whatever the operator typed. Runs *before* validation, which is why it tolerates nulls and
    /// deliberately does not clamp the prompt list - silently dropping a fifth prompt would hide an error the
    /// operator should see.
    public static WidgetTheme Normalize(WidgetTheme theme) => theme with
    {
        AccentColor = theme.AccentColor?.Trim().ToLowerInvariant() ?? "",
        FontFamily = theme.FontFamily?.Trim() ?? "",
        HeaderTitle = theme.HeaderTitle?.Trim() ?? "",
        HeaderSubtitle = Blank(theme.HeaderSubtitle),
        AvatarInitials = Blank(theme.AvatarInitials)?.ToUpperInvariant(),
        GreetingTitle = Blank(theme.GreetingTitle),
        GreetingBody = Blank(theme.GreetingBody),
        InputPlaceholder = theme.InputPlaceholder?.Trim() ?? "",
        Disclaimer = Blank(theme.Disclaimer),
        SuggestedPrompts = (theme.SuggestedPrompts ?? [])
            .Select(prompt => prompt?.Trim() ?? "")
            .Where(prompt => prompt.Length > 0)
            .ToArray(),
    };

    private static string? Blank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

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

    private static bool TryValidateSuggestedPrompts(string[]? prompts, out string? error)
    {
        if (prompts is null)
        {
            error = null;
            return true;
        }

        if (prompts.Length > MaxSuggestedPrompts)
        {
            error = $"suggestedPrompts must contain {MaxSuggestedPrompts} entries or fewer";
            return false;
        }

        foreach (var prompt in prompts)
        {
            if ((prompt?.Trim().Length ?? 0) > MaxSuggestedPromptLength)
            {
                error = $"each suggested prompt must be {MaxSuggestedPromptLength} characters or fewer";
                return false;
            }
        }

        error = null;
        return true;
    }
}
