using FastTests;
using Raven.Quill.Slack;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlackApiClientTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Angle_brackets_and_ampersands_are_escaped()
    {
        Assert.Equal("use HashMap&lt;String, Integer&gt; &amp; friends", SlackApiClient.Escape("use HashMap<String, Integer> & friends"));
        Assert.Equal("&lt;!here&gt;", SlackApiClient.Escape("<!here>"));
        Assert.Equal("var x = a &lt; b;", SlackApiClient.Escape("var x = a < b;"));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Escaping_never_grows_text_past_the_declared_expansion()
    {
        var worst = new string('&', 100);
        Assert.True(SlackApiClient.Escape(worst).Length <= worst.Length * SlackApiClient.MaxEscapeExpansion);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Empty_input_passes_through()
    {
        Assert.Equal("", SlackApiClient.Escape(""));
    }
}
