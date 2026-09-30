using FastTests;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackMrkdwnTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Converts_double_asterisk_and_double_underscore_bold()
    {
        Assert.Equal("*bold* and *also bold*", SlackMrkdwn.Convert("**bold** and __also bold__"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Converts_markdown_links_to_mrkdwn_links()
    {
        Assert.Equal("see <https://example.org/docs|the docs> here",
            SlackMrkdwn.Convert("see [the docs](https://example.org/docs) here"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Converts_headings_to_bold_lines()
    {
        Assert.Equal("*Title*", SlackMrkdwn.Convert("# Title"));
        Assert.Equal("*Sub heading*", SlackMrkdwn.Convert("### Sub heading"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void A_heading_that_is_already_fully_bold_is_not_double_wrapped()
    {
        Assert.Equal("*Title*", SlackMrkdwn.Convert("# **Title**"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Converts_strikethrough()
    {
        Assert.Equal("~gone~", SlackMrkdwn.Convert("~~gone~~"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Code_fences_pass_through_minus_the_language_tag()
    {
        var converted = SlackMrkdwn.Convert("intro **b**\n```csharp\nvar **x** = [a](https://x.example);\n```\noutro **b**");

        Assert.Equal("intro *b*\n```\nvar **x** = [a](https://x.example);\n```\noutro *b*", converted);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Inline_code_content_is_never_rewritten()
    {
        Assert.Equal("use `**not bold**` and *is bold*", SlackMrkdwn.Convert("use `**not bold**` and **is bold**"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Single_emphasis_and_bullets_pass_through()
    {
        var text = "*already mrkdwn bold*\n- a bullet";

        Assert.Equal(text, SlackMrkdwn.Convert(text));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Quotes_are_escaped_into_the_form_slack_renders_as_blockquotes()
    {
        Assert.Equal("&gt; a quote", SlackMrkdwn.Convert("> a quote"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Angle_brackets_and_ampersands_are_escaped()
    {
        Assert.Equal("use HashMap&lt;String, Integer&gt; &amp; friends",
            SlackMrkdwn.Convert("use HashMap<String, Integer> & friends"));
        Assert.Equal("&lt;!here&gt;", SlackMrkdwn.Convert("<!here>"));
        Assert.Equal("var x = a &lt; b;", SlackMrkdwn.Escape("var x = a < b;"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Escaping_does_not_break_generated_links()
    {
        Assert.Equal("<https://example.org/?a=1&amp;b=2|docs>",
            SlackMrkdwn.Convert("[docs](https://example.org/?a=1&b=2)"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Empty_input_passes_through()
    {
        Assert.Equal("", SlackMrkdwn.Convert(""));
    }
}
