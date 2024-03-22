using System.Runtime.InteropServices;
using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenFactAttribute : FactAttribute, ITraitAttribute
{
    private string _skip;
    private readonly RavenTestCategory _category;
    public RavenFactAttribute(RavenTestCategory category)
    {
        _category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool MsSqlRequired { get; set; }

    public bool ElasticSearchRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;
            if (RuntimeInformation.ProcessArchitecture == Architecture.X86)
            {
                if (_category.HasFlag(RavenTestCategory.Sharding))
                    return RavenDataAttributeBase.ShardingSkipMessage;
            }

            if (LicenseRequiredFactAttribute.ShouldSkip(LicenseRequired))
                return LicenseRequiredFactAttribute.SkipMessage;

            if (RequiresMsSqlFactAttribute.ShouldSkip(MsSqlRequired, out skip))
                return skip;
            
            if (RequiresElasticSearchRetryFactAttribute.ShouldSkip(ElasticSearchRequired, out skip))
                return skip;

            if (NightlyBuildFactAttribute.ShouldSkip(NightlyBuildRequired, out skip))
                return skip;

            return null;
        }

        set => _skip = value;
    }
}
