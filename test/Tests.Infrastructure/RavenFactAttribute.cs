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
    
    public bool AzureQueueStorageRequired {get; set; }

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

            if (LicenseRequired && LicenseRequiredFactAttribute.ShouldSkip())
                return LicenseRequiredFactAttribute.SkipMessage;

            if (MsSqlRequired && RequiresMsSqlFactAttribute.ShouldSkip(out skip))
                return skip;

            if (ElasticSearchRequired && RequiresElasticSearchRetryFactAttribute.ShouldSkip(out skip))
                return skip;

            if (NightlyBuildRequired && NightlyBuildFactAttribute.ShouldSkip(out skip))
                return skip;

            if (AzureQueueStorageRequired && AzureQueueStorageHelper.ShouldSkip(out skip))
                return skip;

            return null;
        }

        set => _skip = value;
    }
}
