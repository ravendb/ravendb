using Xunit;

namespace Tests.Infrastructure;

public class MultiplatformTheoryAttribute : TheoryAttribute
{
    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;

    private string _skip;

    public MultiplatformTheoryAttribute(RavenPlatform platform = RavenPlatform.All)
        : this(platform, RavenArchitecture.All)
    {
    }

    public MultiplatformTheoryAttribute(RavenArchitecture architecture = RavenArchitecture.All)
        : this(RavenPlatform.All, architecture)
    {
    }

    public MultiplatformTheoryAttribute(RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All)
    {
        _platform = platform;
        _architecture = architecture;
    }

    public bool LicenseRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            return MultiplatformFactAttribute.ShouldSkip(_platform, _architecture, LicenseRequired);
        }
        set => _skip = value;
    }
}
