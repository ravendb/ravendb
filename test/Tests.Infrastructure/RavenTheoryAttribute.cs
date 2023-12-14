using System.Runtime.InteropServices;
using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    private string _skip;
    internal const string CoraxSkipMessage = $"Corax tests are skipped on Architecture.X86";
    internal const string ShardingSkipMessage = $"Sharding tests are skipped on Architecture.X86";
    private readonly RavenTestCategory _category;
    public RavenTheoryAttribute(RavenTestCategory category)
    {
        _category = category;
    }

    public bool LicenseRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            if (RuntimeInformation.ProcessArchitecture == Architecture.X86)
            {
                if (_category.HasFlag(RavenTestCategory.Corax))
                    return RavenTheoryAttribute.CoraxSkipMessage;
                if (_category.HasFlag(RavenTestCategory.Sharding))
                    return RavenTheoryAttribute.ShardingSkipMessage;
            }

            if (LicenseRequiredFactAttribute.ShouldSkip(LicenseRequired))
                return LicenseRequiredFactAttribute.SkipMessage;

            return null;
        }

        set => _skip = value;
    }
}
