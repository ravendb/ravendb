using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    private string _skip;

    public RavenTheoryAttribute(RavenTestCategory category)
    {
    }

    public bool LicenseRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            if (LicenseRequiredFactAttribute.ShouldSkip(LicenseRequired))
                return LicenseRequiredFactAttribute.SkipMessage;

            return null;
        }

        set => _skip = value;
    }
}
