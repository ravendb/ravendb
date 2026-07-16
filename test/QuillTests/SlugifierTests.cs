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

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("my-app",  true)]
    [InlineData("a2",      true)]
    [InlineData("a",       true)]
    [InlineData(null,      false)]
    [InlineData("",        false)]
    [InlineData("My-App",  false)]
    [InlineData("a_b",     false)]
    [InlineData("-a",      false)]
    [InlineData("a-",      false)]
    [InlineData("a--b",    false)]
    [InlineData("a b",     false)]
    public void IsWellFormed_accepts_only_canonical_slugs(string? input, bool expected)
    {
        Assert.Equal(expected, Slugifier.IsWellFormed(input));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void IsWellFormed_enforces_max_length()
    {
        Assert.True(Slugifier.IsWellFormed(new string('a', Slugifier.MaxLength)));
        Assert.False(Slugifier.IsWellFormed(new string('a', Slugifier.MaxLength + 1)));
    }
}
