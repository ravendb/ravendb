using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenFactAttribute : FactAttribute, ITraitAttribute
{
    public readonly RavenTestCategory Category;
    private string _skip;

    public RavenFactAttribute(RavenTestCategory category)
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
                return RavenTheoryAttribute.CoraxSkipMessage;
            
            if (LicenseRequiredFactAttribute.ShouldSkip(LicenseRequired))
                return LicenseRequiredFactAttribute.SkipMessage;

            return null;
        }

        set => _skip = value;
    }
}
