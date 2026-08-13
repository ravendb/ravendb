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
    /// A mutable stand-in for the record so each test states only the field it is about. Public because the
    /// invalid-theme theory data hands tweaks to xUnit.
    public sealed class WidgetThemeBuilder
    {
        public WidgetAppearance Appearance = WidgetAppearance.Light;
        public string AccentColor = "#2f6f4f";
        public int Radius = 10;
        public string FontFamily = WidgetFonts.SystemStack;
        public WidgetDensity Density = WidgetDensity.Comfortable;
        public string HeaderTitle = "Support";
        public string? HeaderSubtitle = "We usually reply instantly";
        public string? AvatarInitials = "sp";
        public bool ShowHeader = true;
        public string? GreetingTitle = "Hi there";
        public string? GreetingBody = "Ask us anything about your order.";
        public string[] SuggestedPrompts = ["Where is my order?", "How do I return an item?"];
        public string InputPlaceholder = "Type a message...";
        public string? Disclaimer = "AI responses may be inaccurate.";

        public WidgetTheme Build() => new(
            Appearance, AccentColor, Radius, FontFamily, Density, HeaderTitle, HeaderSubtitle, AvatarInitials,
            ShowHeader, GreetingTitle, GreetingBody, SuggestedPrompts, InputPlaceholder, Disclaimer);
    }

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

    private async Task<Widget> NewWidgetAsync()
    {
        var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "demo", Name = "Demo", SystemPrompt = "You help.",
            ConnectionStringName = Host.ConnectionStringName,
        });
        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo", ["http://shop.example"], "Support Widget"));

        return new Widget(app, channel.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_fresh_widget_follows_the_app_default()
    {
        await using var widget = await NewWidgetAsync();

        var response = await Host.GetWidgetThemeAsync(widget.Slug, widget.ChannelId);

        Assert.Null(response.Theme);
        Assert.Equal(WidgetTheme.Default.AccentColor, response.DefaultTheme.AccentColor);
        Assert.NotEmpty(response.FontOptions);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Saving_a_theme_then_clearing_it_returns_to_the_app_default()
    {
        await using var widget = await NewWidgetAsync();

        var saved = await Host.UpdateWidgetThemeAsync(
            widget.Slug, widget.ChannelId, new UpdateWidgetThemeRequest(Sample()));
        Assert.Equal("#2f6f4f", saved.Theme!.AccentColor);

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
                theme.AccentColor = "  #2F6F4F ";
                theme.AvatarInitials = " sp ";
                theme.HeaderSubtitle = "   ";
                theme.SuggestedPrompts = ["  keep me  ", "   ", ""];
            })));

        Assert.Equal("#2f6f4f", saved.Theme!.AccentColor);
        Assert.Equal("SP", saved.Theme.AvatarInitials);
        Assert.Null(saved.Theme.HeaderSubtitle);
        Assert.Equal(["keep me"], saved.Theme.SuggestedPrompts);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task The_app_default_applies_to_widgets_that_make_no_choice()
    {
        await using var widget = await NewWidgetAsync();

        await Host.UpdateWidgetDefaultThemeAsync(widget.Slug,
            new UpdateWidgetThemeRequest(Sample(theme => theme.AccentColor = "#123456")));

        var response = await Host.GetWidgetThemeAsync(widget.Slug, widget.ChannelId);

        Assert.Null(response.Theme);
        Assert.Equal("#123456", response.DefaultTheme.AccentColor);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Clearing_the_app_default_resets_it_to_the_built_in()
    {
        await using var widget = await NewWidgetAsync();

        await Host.UpdateWidgetDefaultThemeAsync(widget.Slug,
            new UpdateWidgetThemeRequest(Sample(theme => theme.AccentColor = "#123456")));
        var reset = await Host.UpdateWidgetDefaultThemeAsync(widget.Slug, new UpdateWidgetThemeRequest(null));

        Assert.Equal(WidgetTheme.Default.AccentColor, reset.Theme.AccentColor);
    }

    public static TheoryData<string, Action<WidgetThemeBuilder>> InvalidThemes() => new()
    {
        { "accentColor", theme => theme.AccentColor = "rebeccapurple" },
        { "accentColor", theme => theme.AccentColor = "#12345" },
        { "radius", theme => theme.Radius = 40 },
        { "radius", theme => theme.Radius = -1 },
        { "fontFamily", theme => theme.FontFamily = "Georgia; background: url(https://evil.example)" },
        { "fontFamily", theme => theme.FontFamily = "Georgia } :root { color: red" },
        { "fontFamily", theme => theme.FontFamily = "@import url(x)" },
        { "fontFamily", theme => theme.FontFamily = "" },
        { "suggestedPrompts", theme => theme.SuggestedPrompts = ["a", "b", "c", "d", "e"] },
        { "suggested prompt", theme => theme.SuggestedPrompts = [new string('x', 81)] },
        { "headerTitle", theme => theme.HeaderTitle = "" },
        { "headerTitle", theme => theme.HeaderTitle = new string('x', 61) },
        { "inputPlaceholder", theme => theme.InputPlaceholder = " " },
        { "disclaimer", theme => theme.Disclaimer = new string('x', 201) },
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
