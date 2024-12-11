using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    internal const string CoraxSkipMessage = $"Corax tests are skipped on v5.4";

    public readonly RavenTestCategory Category;
    private string _skip;

    public RavenTheoryAttribute(RavenTestCategory category)
    {
        Category = category;
    }

    public bool LicenseRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            if (Category.HasFlag(RavenTestCategory.Corax))
            {
                return CoraxSkipMessage;
            }

            if (LicenseRequiredFactAttribute.ShouldSkip(LicenseRequired))
                return LicenseRequiredFactAttribute.SkipMessage;

            return null;
        }

        set => _skip = value;
    }
}
