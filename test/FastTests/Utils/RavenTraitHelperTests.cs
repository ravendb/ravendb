using System.Linq;
using Tests.Infrastructure;
using Tests.Infrastructure.XunitExtensions;
using Xunit;

namespace FastTests.Utils;

public class RavenTraitHelperTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Codebase)]
    public void Emits_single_category()
    {
        AssertTraits(
            RavenTestCategory.Querying,
            nameof(RavenTestCategory.Querying));
    }

    [RavenFact(RavenTestCategory.Codebase)]
    public void Emits_two_categories()
    {
        AssertTraits(
            RavenTestCategory.Querying | RavenTestCategory.Indexes,
            nameof(RavenTestCategory.Querying),
            nameof(RavenTestCategory.Indexes));
    }

    [RavenFact(RavenTestCategory.Codebase)]
    public void Emits_three_categories()
    {
        AssertTraits(
            RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax,
            nameof(RavenTestCategory.Querying),
            nameof(RavenTestCategory.Indexes),
            nameof(RavenTestCategory.Corax));
    }

    private static void AssertTraits(RavenTestCategory category, params string[] expectedCategoryNames)
    {
        var traits = RavenTraitHelper.GetTraitsFor(category);

        Assert.Equal(expectedCategoryNames.Length, traits.Count);

        // Every emitted trait must use the "Category" key.
        Assert.All(traits, trait => Assert.Equal("Category", trait.Key));

        // The emitted category names must match exactly (order-independent, duplicates would fail).
        var emitted = traits.Select(trait => trait.Value);
        Assert.Equal(expectedCategoryNames.OrderBy(name => name), emitted.OrderBy(name => name));
    }
}
