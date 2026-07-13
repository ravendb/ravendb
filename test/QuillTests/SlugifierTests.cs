using FastTests;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class SlugifierTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("Northwind Demo",   "northwind-demo")]
    [InlineData("northwind-demo",   "northwind-demo")]   // already a slug
    [InlineData("ACME SHOP",        "acme-shop")]
    [InlineData("Acme Shop!! 2",    "acme-shop-2")]      // punctuation collapses to single dash
    [InlineData("  spaces   ok  ",  "spaces-ok")]        // leading/trailing/repeated whitespace
    [InlineData("a/b\\c",           "a-b-c")]            // slashes -> dash
    [InlineData("café",             "caf")]              // non-ASCII letter dropped (no transliteration)
    [InlineData("under_score.dot",  "under-score-dot")]  // _ and . treated as separators
    public void ToSlug_normalizes_input(string input, string expected)
    {
        Assert.Equal(expected, Slugifier.ToSlug(input));
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("----")]   // no alphanumeric -> nothing accumulates
    [InlineData("!!@@##")] // same -- pure punctuation
    public void ToSlug_returns_empty_when_input_has_no_alphanumeric(string? input)
    {
        Assert.Equal(string.Empty, Slugifier.ToSlug(input));
    }
}
