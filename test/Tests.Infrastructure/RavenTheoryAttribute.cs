using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    public RavenTheoryAttribute(RavenTestCategory category)
    {
    }

    public bool LicenseRequired { get; set; }

    public override string Skip
    {
        get
        {
            if (LicenseRequired && LicenseRequiredFactAttribute.ShouldSkip(licenseRequired: true)) 
                return LicenseRequiredFactAttribute.SkipMessage;

            return base.Skip;
        }
    }
}
