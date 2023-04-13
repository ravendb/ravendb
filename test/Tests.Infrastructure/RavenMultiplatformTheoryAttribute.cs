namespace Tests.Infrastructure;

public class RavenMultiplatformTheoryAttribute : RavenTheoryAttribute
{
    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;

    private string _skip;

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform = RavenPlatform.All)
        : this(category, platform, RavenArchitecture.All)
    {
    }

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenArchitecture architecture = RavenArchitecture.All)
        : this(category, RavenPlatform.All, architecture)
    {
    }

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform = RavenPlatform.All, RavenArchitecture architecture = RavenArchitecture.All)
        : base(category)
    {
        _platform = platform;
        _architecture = architecture;
    }

    public bool NightlyBuildOnly { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            return RavenMultiplatformFactAttribute.ShouldSkip(_platform, _architecture, LicenseRequired, NightlyBuildOnly);
        }
        set => _skip = value;
    }
}
