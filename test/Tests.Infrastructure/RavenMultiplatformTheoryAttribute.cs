namespace Tests.Infrastructure;

public class RavenMultiplatformTheoryAttribute : RavenTheoryAttribute, Xunit.v3.IFactAttribute
{
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

    private readonly RavenPlatform _platform;
    private readonly RavenArchitecture _architecture;
    private readonly RavenIntrinsics _intrinsics;

    private string _skip;

   
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category)
        : this(category, RavenPlatform.All)
    {
    }
    
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenIntrinsics intrinsics)
        : this(category, RavenPlatform.All, RavenArchitecture.All, intrinsics)
    {
    }

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform)
        : this(category, platform, RavenArchitecture.All, RavenIntrinsics.None)
    {
    }
    
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenArchitecture architecture)
        : this(category, RavenPlatform.All, architecture, RavenIntrinsics.None)
    {
    }

    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform, RavenArchitecture architecture)
        : this(category, platform, architecture, RavenIntrinsics.None)
    {
    }
    
    public RavenMultiplatformTheoryAttribute(RavenTestCategory category, RavenPlatform platform, RavenArchitecture architecture, RavenIntrinsics intrinsics)
        : base(category)
    {
        _platform = platform;
        _architecture = architecture;
        _intrinsics = intrinsics;
    }

    public new string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            return RavenMultiplatformFactAttribute.ShouldSkip(_platform, _architecture, _intrinsics, LicenseRequired, NightlyBuildRequired, SnowflakeRequired);
        }
        set => _skip = value;
    }
}
