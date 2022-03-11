using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenFactAttribute : FactAttribute, ITraitAttribute
{
    public RavenFactAttribute(RavenTestCategory category)
    {
    }
}
