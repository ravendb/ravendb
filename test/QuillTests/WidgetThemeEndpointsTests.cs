using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class WidgetThemeEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private const string TinyPngLogo = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg==";

    /// A mutable stand-in for the record so each test states only the field it is about. Public because the
    /// invalid-theme theory data hands tweaks to xUnit.
    public sealed class WidgetThemeBuilder
    {
        public WidgetAppearance Appearance = WidgetAppearance.Light;
        public WidgetThemeColors Light = new(ButtonColor: "#2f6f4f", MessageColor: "#e6f0ea", BackgroundColor: "#ffffff");
        public WidgetThemeColors Dark = new(ButtonColor: "#2f6f4f", MessageColor: "#1c2b23", BackgroundColor: "#0d1117");
        public WidgetRadius Radius = WidgetRadius.Medium;
        public string FontFamily = WidgetFonts.SystemStack;
        public WidgetFontSize FontSize = WidgetFontSize.Medium;
        public double? CustomFontSizeRem;
        public string? Logo;
        public WidgetLogoRadius LogoRadius = WidgetLogoRadius.Pill;
        public string HeaderTitle = "Support";
        public string? HeaderSubtitle = "We usually reply instantly";
        public bool ShowHeader = true;
        public string? GreetingTitle = "Hi there";
        public string? GreetingBody = "Ask us anything about your order.";
        public string[] SuggestedPrompts = ["Where is my order?", "How do I return an item?"];
        public string InputPlaceholder = "Type a message...";
        public string? Disclaimer = "AI responses may be inaccurate.";
        public string? CustomCss;

        public WidgetTheme Build() => new(
            Appearance, Light, Dark, Radius, FontFamily, FontSize, CustomFontSizeRem, Logo, LogoRadius,
            HeaderTitle, HeaderSubtitle, ShowHeader, GreetingTitle, GreetingBody, SuggestedPrompts,
            InputPlaceholder, Disclaimer, CustomCss);
    }

    /// Derived from the limit rather than written out, so raising it does not silently turn an
    /// "over the limit" case into an "at the limit" one.
    private static string[] Prompts(int count) =>
        Enumerable.Range(1, count).Select(i => $"prompt {i}").ToArray();

    private static WidgetTheme Sample(Action<WidgetThemeBuilder>? tweak = null)
    {
        var builder = new WidgetThemeBuilder();
        tweak?.Invoke(builder);
        return builder.Build();
    }

    private sealed record Widget(QuillApp App, string ChannelId) : IAsyncDisposable
    {
        public string Slug => App.Slug;
        public ValueTask DisposeAsync() => App.DisposeAsync();
    }

    private async Task<Widget> NewWidgetAsync(string displayName = "Support Widget")
    {
        var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.",
            ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo", ["http://shop.example"], displayName));

        return new Widget(app, channel.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_fresh_widget_follows_the_app_default()
    {
        await using var widget = await NewWidgetAsync();

        var response = await Host.GetWidgetThemeAsync(widget.Slug, widget.ChannelId);

        Assert.Null(response.Theme);
        Assert.Equal(WidgetTheme.Default.Light, response.DefaultTheme.Light);
        Assert.Equal(WidgetTheme.Default.Dark, response.DefaultTheme.Dark);
        Assert.NotEmpty(response.FontOptions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Saving_a_theme_then_clearing_it_returns_to_the_app_default()
    {
        await using var widget = await NewWidgetAsync();

        var saved = await Host.UpdateWidgetThemeAsync(
            widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(Sample()));
        Assert.Equal("#2f6f4f", saved.Theme!.Light.ButtonColor);

        var cleared = await Host.UpdateWidgetThemeAsync(
            widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(null));
        Assert.Null(cleared.Theme);

        Assert.Null((await Host.GetWidgetThemeAsync(widget.Slug, widget.ChannelId)).Theme);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Saving_normalizes_whitespace_and_case()
    {
        await using var widget = await NewWidgetAsync();

        var saved = await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(
            Sample(theme =>
            {
                theme.Light = theme.Light with { ButtonColor = "  #2F6F4F " };
                theme.HeaderSubtitle = "   ";
                theme.SuggestedPrompts = ["  keep me  ", "   ", ""];
                theme.CustomCss = "   ";
                // ignored because FontSize is not Custom; normalization drops it
                theme.CustomFontSizeRem = 1.25;
            })));

        Assert.Equal("#2f6f4f", saved.Theme!.Light.ButtonColor);
        Assert.Null(saved.Theme.HeaderSubtitle);
        Assert.Equal(["keep me"], saved.Theme.SuggestedPrompts);
        Assert.Null(saved.Theme.CustomCss);
        Assert.Null(saved.Theme.CustomFontSizeRem);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_app_default_applies_to_widgets_that_make_no_choice()
    {
        await using var widget = await NewWidgetAsync();

        await Host.UpdateWidgetDefaultThemeAsync(widget.Slug, new UpdateWidgetThemeRequest(
            Sample(theme => theme.Light = theme.Light with { ButtonColor = "#123456" })));

        var response = await Host.GetWidgetThemeAsync(widget.Slug, widget.ChannelId);

        Assert.Null(response.Theme);
        Assert.Equal("#123456", response.DefaultTheme.Light.ButtonColor);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Clearing_the_app_default_resets_it_to_the_built_in()
    {
        await using var widget = await NewWidgetAsync();

        await Host.UpdateWidgetDefaultThemeAsync(widget.Slug, new UpdateWidgetThemeRequest(
            Sample(theme => theme.Light = theme.Light with { ButtonColor = "#123456" })));
        var reset = await Host.UpdateWidgetDefaultThemeAsync(widget.Slug, new UpdateWidgetThemeRequest(null));

        Assert.Equal(WidgetTheme.Default.Light, reset.Theme.Light);
    }

    /// A display name may be more than three times as long as a header title, and a header-off theme is
    /// allowed to leave the title blank - so the fill-in for one has to stay inside the bounds of the
    /// other. It resolves into the embed document, and a theme that fails its own validation there is
    /// discarded whole, which would cost the operator every colour they picked.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_long_display_name_does_not_cost_a_header_off_widget_its_theme()
    {
        var displayName = new string('x', WidgetThemeValidation.MaxHeaderTitleLength + 20);
        await using var widget = await NewWidgetAsync(displayName);

        await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(
            Sample(theme =>
            {
                theme.ShowHeader = false;
                theme.HeaderTitle = "";
                theme.Light = theme.Light with { ButtonColor = "#123456" };
            })));

        var token = (await widget.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(widget.ChannelId))).Token;
        var html = await Host.GetEmbedPageAsync(widget.Slug, token);

        Assert.Contains("#123456", html);
        Assert.DoesNotContain(WidgetTheme.Default.Light.ButtonColor, html);
        Assert.Contains($"<title>{displayName[..WidgetThemeValidation.MaxHeaderTitleLength]}</title>", html);
    }

    public static TheoryData<string, Action<WidgetThemeBuilder>> InvalidThemes() => new()
    {
        { "light.buttonColor", theme => theme.Light = theme.Light with { ButtonColor = "rebeccapurple" } },
        { "light.messageColor", theme => theme.Light = theme.Light with { MessageColor = "#12345" } },
        { "dark.backgroundColor", theme => theme.Dark = theme.Dark with { BackgroundColor = "rgb(0,0,0)" } },
        { "logo", theme => theme.Logo = "data:image/svg+xml;base64,PHN2Zz48L3N2Zz4=" },
        { "logo", theme => theme.Logo = "data:image/png;base64," + new string('A', WidgetThemeValidation.MaxLogoLength) },
        { "customCss", theme => theme.CustomCss = ".rq-root { color: red } </StYlE><script>alert(1)</script>" },
        { "customCss", theme => theme.CustomCss = new string('a', WidgetThemeValidation.MaxCustomCssLength + 1) },
        { "customFontSizeRem", theme => { theme.FontSize = WidgetFontSize.Custom; theme.CustomFontSizeRem = null; } },
        { "customFontSizeRem", theme => { theme.FontSize = WidgetFontSize.Custom; theme.CustomFontSizeRem = 0.5; } },
        { "customFontSizeRem", theme => { theme.FontSize = WidgetFontSize.Custom; theme.CustomFontSizeRem = 1.6; } },
        { "fontFamily", theme => theme.FontFamily = "Georgia; background: url(https://evil.example)" },
        { "fontFamily", theme => theme.FontFamily = "Georgia } :root { color: red" },
        { "fontFamily", theme => theme.FontFamily = "@import url(x)" },
        { "fontFamily", theme => theme.FontFamily = "" },
        { "fontFamily", theme => theme.FontFamily = new string('a', WidgetThemeValidation.MaxFontFamilyLength + 1) },
        { "radius", theme => theme.Radius = (WidgetRadius)99 },
        { "logoRadius", theme => theme.LogoRadius = (WidgetLogoRadius)99 },
        { "suggestedPrompts", theme => theme.SuggestedPrompts = Prompts(WidgetThemeValidation.MaxSuggestedPrompts + 1) },
        { "suggested prompt", theme => theme.SuggestedPrompts = [new string('x', WidgetThemeValidation.MaxSuggestedPromptLength + 1)] },
        { "headerTitle", theme => theme.HeaderTitle = "" },
        { "headerTitle", theme => theme.HeaderTitle = new string('x', WidgetThemeValidation.MaxHeaderTitleLength + 1) },
        { "inputPlaceholder", theme => theme.InputPlaceholder = " " },
        { "disclaimer", theme => theme.Disclaimer = new string('x', WidgetThemeValidation.MaxDisclaimerLength + 1) },
    };

    [RavenTheory(RavenTestCategory.Quill)]
    [MemberData(nameof(InvalidThemes))]
    public async Task Invalid_themes_are_rejected(string expectedInMessage, Action<WidgetThemeBuilder> tweak)
    {
        await using var widget = await NewWidgetAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() =>
            Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(Sample(tweak))));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains(expectedInMessage, ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_hand_written_font_stack_is_accepted_when_it_only_uses_font_stack_characters()
    {
        await using var widget = await NewWidgetAsync();

        var saved = await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(
            Sample(theme => theme.FontFamily = """Inter, "Noto Sans", Arial, sans-serif""")));

        Assert.Equal("""Inter, "Noto Sans", Arial, sans-serif""", saved.Theme!.FontFamily);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Every_curated_font_stack_is_accepted_verbatim()
    {
        await using var widget = await NewWidgetAsync();

        foreach (var option in WidgetFonts.Curated)
        {
            var saved = await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId,
                new UpdateWidgetThemeRequest(Sample(theme => theme.FontFamily = option.Stack)));

            Assert.Equal(option.Stack, saved.Theme!.FontFamily);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_raster_logo_and_a_custom_font_size_are_accepted()
    {
        await using var widget = await NewWidgetAsync();

        var saved = await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(
            Sample(theme =>
            {
                theme.Logo = TinyPngLogo;
                theme.FontSize = WidgetFontSize.Custom;
                theme.CustomFontSizeRem = 1.25;
            })));

        Assert.Equal(TinyPngLogo, saved.Theme!.Logo);
        Assert.Equal(WidgetFontSize.Custom, saved.Theme.FontSize);
        Assert.Equal(1.25, saved.Theme.CustomFontSizeRem);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_hidden_header_does_not_require_a_title()
    {
        await using var widget = await NewWidgetAsync();

        var saved = await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(
            Sample(theme =>
            {
                theme.ShowHeader = false;
                theme.HeaderTitle = "";
                theme.HeaderSubtitle = null;
            })));

        Assert.False(saved.Theme!.ShowHeader);
        Assert.Equal("", saved.Theme.HeaderTitle);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Blank_prompts_do_not_count_toward_the_prompt_limit()
    {
        await using var widget = await NewWidgetAsync();
        var full = Prompts(WidgetThemeValidation.MaxSuggestedPrompts);

        var saved = await Host.UpdateWidgetThemeAsync(widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(
            Sample(theme => theme.SuggestedPrompts = [.. full, "   "])));

        Assert.Equal(full, saved.Theme!.SuggestedPrompts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_theme_route_404s_for_a_non_widget_channel()
    {
        await using var app = await NewAppAsync();

        // Seeded directly: the API can't provision a Telegram channel (501).
        const string channelId = "telegram-1";
        using (var session = app.Store.OpenAsyncSession())
        {
            await session.StoreAsync(new Channel
            {
                Id = Channel.IdPrefix + channelId,
                Type = ChannelType.Telegram,
                DisplayName = "Telegram",
                AgentId = "demo",
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetWidgetThemeAsync(app.Slug, channelId));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
